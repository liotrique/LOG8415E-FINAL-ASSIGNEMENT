from dataclasses import dataclass
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
        self, commands: list[str], instances: list[EC2Instance]
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
        my_sql_and_sakila_commands = [
            # Update and Install MySQL Server
            "sudo apt-get update",
            "sudo apt-get install -y mysql-server wget sysbench",
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
        ]
        # Execute commands
        self.execute_commands(
            my_sql_and_sakila_commands, [self.manager_instance] + self.worker_instances
        )

    def run_sys_bench(self):
        sysbench_commands = [
            "sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user='root' --mysql-password='root_password' prepare",
            "sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user='root' --mysql-password='root_password' run > sysbench_results.txt",
        ]
        # Execute commands
        self.execute_commands(
            sysbench_commands, [self.manager_instance] + self.worker_instances
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

                scp.close()
                ssh_client.close()
        except Exception as e:
            print(f"An error occurred: {e}")

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

# Install dependencies common to each instance
ec2_manager.install_common_dependencies()
ec2_manager.run_sys_bench()
ec2_manager.save_sys_bench_results()

# Cleanup
press_touched = input("Press any key to terminate and cleanup: ")
ec2_manager.cleanup(all_instances)
print("Cleanup complete.")
