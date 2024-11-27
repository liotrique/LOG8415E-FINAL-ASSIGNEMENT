from dataclasses import dataclass
import json
import boto3
from botocore.exceptions import ClientError
import paramiko
from scp import SCPClient
import time
from typing import Any
import os


@dataclass
class EC2Instance:
    instance: Any
    name: str

    def get_name(self):
        return f"{self.name}_{self.instance.id}"


# Function to create an SSH client
def create_ssh_client(host, user, key_path):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, username=user, key_filename=key_path)
    return ssh


class EC2Manager:

    def __init__(self):

        self.key_name = "temp_key_pair"

        # Clients and resources
        self.ec2_client = boto3.client("ec2", region_name="us-east-1")
        self.ec2_resource = boto3.resource("ec2", region_name="us-east-1")

        # Ids
        self.vpc_id = self.ec2_client.describe_vpcs()["Vpcs"][0]["VpcId"]
        self.ami_id = self._get_latest_ubuntu_ami()

        self.security_group_id = self.ec2_client.create_security_group(
            Description="Common security group",
            GroupName="common_sg",
            VpcId=self.vpc_id,
        )["GroupId"]

        self._add_inboud_rule_security_group()
        self.ssh_key_path = os.path.expanduser(f"./{self.key_name}.pem")

        # All instances
        self.manager_instance: EC2Instance | None = None
        self.worker_instances: list[EC2Instance] | None = []
        self.proxy_instance: EC2Instance | None = None

    def create_key_pair(self) -> None:
        response = self.ec2_client.create_key_pair(KeyName=self.key_name)
        private_key = response["KeyMaterial"]
        with open(f"{self.key_name}.pem", "w") as file:
            file.write(private_key)

    def launch_instances(self) -> list[EC2Instance]:
        """
        Launch instance
        """
        # Launch worker instances
        for i in range(2):
            self.worker_instances.append(
                EC2Instance(
                    self.ec2_resource.create_instances(
                        ImageId=self.ami_id,
                        InstanceType="t2.micro",
                        MinCount=1,
                        MaxCount=1,
                        SecurityGroupIds=[self.security_group_id],
                        KeyName=self.key_name,
                        BlockDeviceMappings=[
                            {
                                "DeviceName": "/dev/sda1",
                                "Ebs": {
                                    "VolumeSize": 16,
                                    "VolumeType": "gp3",
                                    "DeleteOnTermination": True,
                                },
                            }
                        ],
                    )[0],
                    name=f"worker{i + 1}",
                )
            )

        # Launch manager instance
        self.manager_instance = EC2Instance(
            self.ec2_resource.create_instances(
                ImageId=self.ami_id,
                InstanceType="t2.micro",
                MinCount=1,
                MaxCount=1,
                SecurityGroupIds=[self.security_group_id],
                KeyName=self.key_name,
                BlockDeviceMappings=[
                    {
                        "DeviceName": "/dev/sda1",
                        "Ebs": {
                            "VolumeSize": 16,
                            "VolumeType": "gp3",
                            "DeleteOnTermination": True,
                        },
                    }
                ],
            )[0],
            name="manager",
        )

        # Launch proxy instance
        proxy_instance = EC2Instance(
            self.ec2_resource.create_instances(
                ImageId=self.ami_id,
                InstanceType="t2.large",
                MinCount=1,
                MaxCount=1,
                SecurityGroupIds=[self.security_group_id],
                KeyName=self.key_name,
                BlockDeviceMappings=[
                    {
                        "DeviceName": "/dev/sda1",
                        "Ebs": {
                            "VolumeSize": 16,
                            "VolumeType": "gp3",
                            "DeleteOnTermination": True,
                        },
                    }
                ],
            )[0],
            name="proxy",
        )

        return self.worker_instances + [self.manager_instance, proxy_instance]

    def execute_commands(
        self,
        commands: list[str],
        instances: list[EC2Instance],
        print_output: bool = True,
    ) -> None:
        """
        This function executes a list of commands on each instance.
        You can call this function to run any set of commands.
        """
        try:
            for ec2_instance in instances:
                # Connect to the instance
                ssh_client = create_ssh_client(
                    ec2_instance.instance.public_ip_address, "ubuntu", self.ssh_key_path
                )

                # Run the commands
                for command in commands:
                    print(
                        f"Executing command: {command} on instance {ec2_instance.get_name()}"
                    )
                    stdin, stdout, stderr = ssh_client.exec_command(command)

                    # Process output in real-time
                    for line in iter(stdout.readline, ""):
                        if print_output:
                            print(line, end="")  # Print each line from stdout
                    error_output = stderr.read().decode()  # Capture any error output

                    # Wait for command to complete
                    exit_status = stdout.channel.recv_exit_status()
                    if exit_status != 0:
                        print(
                            f"Command '{command}' failed with exit status {exit_status}. Error:\n{error_output}"
                        )
                ssh_client.close()
                time.sleep(2)

        except Exception as e:
            print(f"An error occurred: {e}")

    def install_common_dependencies(self):
        commands = [
            # Update and Install MySQL, sysbench, and Flask
            "sudo apt-get update",
            "sudo apt-get install -y mysql-server wget sysbench python3-pip",
            "sudo pip3 install flask mysql-connector-python requests",
            # Set MySQL root password
            'sudo mysql -e \'ALTER USER "root"@"localhost" IDENTIFIED WITH mysql_native_password BY "root_password";\'',
            # Start and enable MySQL
            "sudo systemctl start mysql",
            "sudo systemctl enable mysql",
            # Download and extract Sakila database
            "wget https://downloads.mysql.com/docs/sakila-db.tar.gz",
            "tar -xzvf sakila-db.tar.gz",
            # Import Sakila schema and data into MySQL
            "sudo mysql -u root -p'root_password' -e 'SOURCE sakila-db/sakila-schema.sql;'",
            "sudo mysql -u root -p'root_password' -e 'SOURCE sakila-db/sakila-data.sql;'",
            # Verify that the Sakila database has been installed correctly
            "sudo mysql -u root -p'root_password' -e 'SHOW DATABASES;'",
            "sudo mysql -u root -p'root_password' -e 'USE sakila; SHOW TABLES;'",
            # Add global environment variables to /etc/environment
            'echo "MYSQL_USER=root" | sudo tee -a /etc/environment',
            'echo "MYSQL_PASSWORD=root_password" | sudo tee -a /etc/environment',
            'echo "MYSQL_DB=sakila" | sudo tee -a /etc/environment',
            'echo "MYSQL_HOST=localhost" | sudo tee -a /etc/environment',
            "source /etc/environment",
        ]

        # Execute commands
        self.execute_commands(
            commands,
            [self.manager_instance] + self.worker_instances,
            print_output=False,
        )

    def run_sys_bench(self):
        sysbench_commands = [
            "sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user='root' --mysql-password='root_password' prepare",
            "sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user='root' --mysql-password='root_password' run > sysbench_results.txt",
        ]
        # Execute commands
        self.execute_commands(
            sysbench_commands,
            [self.manager_instance] + self.worker_instances,
            print_output=False,
        )

    def save_sys_bench_results(self):
        try:
            for ec2_instance in [self.manager_instance] + self.worker_instances:
                # Connect to the instance
                ssh_client = create_ssh_client(
                    ec2_instance.instance.public_ip_address, "ubuntu", self.ssh_key_path
                )

                # Download the sysbench results
                scp = SCPClient(ssh_client.get_transport())
                scp.get(
                    "sysbench_results.txt",
                    f"data/sysbench_results_{ec2_instance.get_name()}.txt",
                )
                print(
                    f"Sysbench results downloaded to data/sysbench_results_{ec2_instance.get_name()}.txt"
                )

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            scp.close()
            ssh_client.close()

    def create_database_and_table(self) -> None:
        """
        Create a database in the manager and the worker instances,
        replacing it if it already exists, and create a table with initial data.
        """
        commands = [
            # Drop the database if it exists, then create a new one
            "sudo mysql -u root -p'root_password' -e 'DROP DATABASE IF EXISTS my_database;'",
            "sudo mysql -u root -p'root_password' -e 'CREATE DATABASE my_database;'",
            # Drop the table if it exists, then create a new table
            "sudo mysql -u root -p'root_password' -e 'USE my_database; DROP TABLE IF EXISTS my_table;'",
            "sudo mysql -u root -p'root_password' -e 'USE my_database; CREATE TABLE my_table (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));'",
            # Insert some data into the table
            'sudo mysql -u root -p\'root_password\' -e \'USE my_database; INSERT INTO my_table (name) VALUES ("Alice"), ("Bob"), ("Charlie");\'',
        ]

        # Execute commands
        self.execute_commands(commands, [self.manager_instance] + self.worker_instances)

    def upload_flask_apps_to_instances(self):
        try:
            # Upload the Flask app to the manager instance
            ssh_client = create_ssh_client(
                self.manager_instance.instance.public_ip_address,
                "ubuntu",
                self.ssh_key_path,
            )
            scp = SCPClient(ssh_client.get_transport())
            scp.put("scripts/manager_script.py", "manager_script.py")
            scp.put("public_ips.json", "public_ips.json")

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            scp.close()
            ssh_client.close()

        # Upload worker script to worker instances
        for worker in self.worker_instances:
            try:
                ssh_client = create_ssh_client(
                    worker.instance.public_ip_address, "ubuntu", self.ssh_key_path
                )
                scp = SCPClient(ssh_client.get_transport())
                scp.put("scripts/worker_script.py", "worker_script.py")
            except Exception as e:
                print(
                    f"Error uploading and starting worker script on {worker.get_name()}: {e}"
                )
            finally:
                scp.close()
                ssh_client.close()

    def start_db_cluster_apps(self):
        # Start the Flask app on the manager instance
        commands = [
            "nohup python3 manager_script.py > manager_output.log 2>&1 &",
        ]
        self.execute_commands(commands, [self.manager_instance])

        # Start the Flask app on the worker instances
        commands = [
            "nohup python3 worker_script.py > worker_output.log 2>&1 &",
        ]
        self.execute_commands(commands, self.worker_instances)

    def cleanup(self, all_instances: list[EC2Instance]):
        """
        Delete the target groups, terminate all instances and delete the security group.

        Raises:
            ClientError: When an error occurs when deleting resources.
        """
        try:
            # Terminate EC2 instance
            instances_ids = [ec2_instance.instance.id for ec2_instance in all_instances]
            self.ec2_client.terminate_instances(InstanceIds=instances_ids)
            print(f"Termination of instances {instances_ids} initiated.")

            waiter = self.ec2_client.get_waiter("instance_terminated")
            waiter.wait(InstanceIds=instances_ids)
            print("Instances terminated.")

            # Delete security group
            self.ec2_client.delete_security_group(GroupId=self.security_group_id)
            print(f"Security group {self.security_group_id} deleted.")

            # Delete key pair
            self.ec2_client.delete_key_pair(KeyName=self.key_name)
            os.remove(self.ssh_key_path)

        except ClientError as e:
            print(f"An error occurred: {e}")

    def _add_inboud_rule_security_group(self):
        """
        Add inbound rules to the security group to allow SSH and application port traffic.
        """
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,  # SSH
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5000,  # Workers and manager listen on port 5000
                    "ToPort": 5000,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
            ],
        )

    def _get_latest_ubuntu_ami(self):
        """
        Get the latest Ubuntu AMI ID.
        """
        response = self.ec2_client.describe_images(
            Filters=[
                {
                    "Name": "name",
                    "Values": [
                        "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"
                    ],
                },
                {"Name": "virtualization-type", "Values": ["hvm"]},
                {"Name": "architecture", "Values": ["x86_64"]},
            ],
            Owners=["099720109477"],  # Canonical
        )
        images = response["Images"]
        images.sort(key=lambda x: x["CreationDate"], reverse=True)
        return images[0]["ImageId"]


# Main

# Launch instances
ec2_manager = EC2Manager()

# Clear data folder
os.system("rm -rf data")
os.system("mkdir data")

ec2_manager.create_key_pair()
time.sleep(5)
all_instances = ec2_manager.launch_instances()

# Wait for instances to be running
print("Waiting for instances to be running...")
for ec2_instance in all_instances:
    ec2_instance.instance.wait_until_running()
    ec2_instance.instance.reload()
    print(f"Instance {ec2_instance.get_name()} is running.")

print("All instances are running.")
time.sleep(10)

# Save public IPs to a JSON file
instance_data = {}

for worker in ec2_manager.worker_instances:
    instance_data[worker.name] = worker.instance.public_ip_address

with open("public_ips.json", "w") as file:
    json.dump(instance_data, file, indent=4)

with open("manager_ip.txt", "w") as file:
    file.write(ec2_manager.manager_instance.instance.public_ip_address)

# Install dependencies common to each instance
print("Installing common dependencies...")
ec2_manager.install_common_dependencies()

print("Running sysbench...")
ec2_manager.run_sys_bench()

print("Saving sysbench results...")
ec2_manager.save_sys_bench_results()

print("Creating database and table...")
ec2_manager.create_database_and_table()

print("Uploading Flask apps to instances...")
ec2_manager.upload_flask_apps_to_instances()

print("Starting Flask apps for the manager and workers...")
ec2_manager.start_db_cluster_apps()


# Cleanup
press_touched = input("Press any key to terminate and cleanup: ")
ec2_manager.cleanup(all_instances)
print("Cleanup complete.")
