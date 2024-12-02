from dataclasses import dataclass
import json
import boto3
from botocore.exceptions import ClientError
import paramiko
import requests
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
    """Create an SSH client using the provided host, user, and key path."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=host, username=user, key_filename=key_path)
    return ssh


# Function to print stats from benchmark
def print_stats(answers: list[dict[str, Any]]) -> None:
    """Print the number of requests and average response time per instance."""

    n_req_per_instance = {"manager": 0, "worker1": 0, "worker2": 0}
    n_time_per_instance = {"manager": 0, "worker1": 0, "worker2": 0}
    for answer in answers:
        if answer["response"]["handled_by"] == "manager":
            n_req_per_instance["manager"] += 1
            n_time_per_instance["manager"] += answer["time"]
        elif answer["response"]["handled_by"] == "worker1":
            n_req_per_instance["worker1"] += 1
            n_time_per_instance["worker1"] += answer["time"]
        elif answer["response"]["handled_by"] == "worker2":
            n_req_per_instance["worker2"] += 1
            n_time_per_instance["worker2"] += answer["time"]

    # Divide time per instance by n istance
    for key in n_time_per_instance:
        if n_req_per_instance[key] > 0:
            n_time_per_instance[key] /= n_req_per_instance[key]

    print("Number of requests per instance:")
    print(n_req_per_instance)
    print("Average response time per instance:")
    print(n_time_per_instance)


class EC2Manager:
    def __init__(self) -> None:
        self.key_name = "temp_key_pair"

        # Clients and resources
        self.ec2_client = boto3.client("ec2", region_name="us-east-1")
        self.ec2_resource = boto3.resource("ec2", region_name="us-east-1")

        # Ids
        self.vpc_id = self.ec2_client.describe_vpcs()["Vpcs"][0]["VpcId"]
        self.ami_id = self._get_latest_ubuntu_ami()

        # Security groups
        self.cluster_security_group_id = self.ec2_client.create_security_group(
            Description="Security group for manager and workers",
            GroupName="common_sg",
            VpcId=self.vpc_id,
        )["GroupId"]

        self.proxy_security_group_id = self.ec2_client.create_security_group(
            Description="Proxy security group",
            GroupName="proxy_sg",
            VpcId=self.vpc_id,
        )["GroupId"]

        self.trusted_host_security_group_id = self.ec2_client.create_security_group(
            Description="Trusted host security group",
            GroupName="trusted_host_sg",
            VpcId=self.vpc_id,
        )["GroupId"]

        self.gatekeeper_security_group_id = self.ec2_client.create_security_group(
            Description="Gatekeeper security group",
            GroupName="gatekeeper_sg",
            VpcId=self.vpc_id,
        )["GroupId"]

        self.ssh_key_path = os.path.expanduser(f"./{self.key_name}.pem")

        # All instances (instanciated in launch_instances)
        self.manager_instance: EC2Instance | None = None
        self.worker_instances: list[EC2Instance] | None = []
        self.proxy_instance: EC2Instance | None = None
        self.gatekeeper_instance: EC2Instance | None = None
        self.trusted_host_instance: EC2Instance | None = None

    def create_key_pair(self) -> None:
        """Create a new key pair and save the private key to a file."""
        response = self.ec2_client.create_key_pair(KeyName=self.key_name)
        private_key = response["KeyMaterial"]
        with open(f"{self.key_name}.pem", "w") as file:
            file.write(private_key)

    def launch_instances(self) -> list[EC2Instance]:
        """
        Launch manager, worker, and proxy instances.
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
                        SecurityGroupIds=[self.cluster_security_group_id],
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
                SecurityGroupIds=[self.cluster_security_group_id],
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
        self.proxy_instance = EC2Instance(
            self.ec2_resource.create_instances(
                ImageId=self.ami_id,
                InstanceType="t2.large",
                MinCount=1,
                MaxCount=1,
                SecurityGroupIds=[self.proxy_security_group_id],
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

        # Launch trusted host instance
        self.trusted_host_instance = EC2Instance(
            self.ec2_resource.create_instances(
                ImageId=self.ami_id,
                InstanceType="t2.large",
                MinCount=1,
                MaxCount=1,
                SecurityGroupIds=[self.trusted_host_security_group_id],
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
            name="trusted_host",
        )

        # Launch gatekeeper instance
        self.gatekeeper_instance = EC2Instance(
            self.ec2_resource.create_instances(
                ImageId=self.ami_id,
                InstanceType="t2.large",
                MinCount=1,
                MaxCount=1,
                SecurityGroupIds=[self.gatekeeper_security_group_id],
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
            name="gatekeeper",
        )

        return self.worker_instances + [
            self.manager_instance,
            self.proxy_instance,
            self.trusted_host_instance,
            self.gatekeeper_instance,
        ]

    def add_inbound_rules(self) -> None:
        """
        Add inbound rules for security groups
        """
        # Allow SSH access to all instances
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.cluster_security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [
                        {
                            "CidrIp": "0.0.0.0/0",  # Allow SSH access from anywhere
                        },
                    ],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5000,
                    "ToPort": 5000,
                    "IpRanges": [  # Allow access from the proxy, and from the manager (for the workers)
                        {
                            "CidrIp": f"{self.manager_instance.instance.public_ip_address}/32"
                        },
                        {
                            "CidrIp": f"{self.proxy_instance.instance.public_ip_address}/32"
                        },
                    ],
                },
            ],
        )
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.proxy_security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5000,
                    "ToPort": 5000,
                    "IpRanges": [
                        {
                            "CidrIp": f"{self.trusted_host_instance.instance.public_ip_address}/32"  # Allow access from the trusted host
                        }
                    ],
                },
            ],
        )
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.trusted_host_security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5000,
                    "ToPort": 5000,
                    "IpRanges": [
                        {
                            "CidrIp": f"{self.gatekeeper_instance.instance.public_ip_address}/32"  # Allow access from the gatekeeper
                        }
                    ],
                },
            ],
        )
        self.ec2_client.authorize_security_group_ingress(
            GroupId=self.gatekeeper_security_group_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                },
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5000,
                    "ToPort": 5000,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],  # Allow access from anywhere
                },
            ],
        )

    def execute_commands(
        self,
        commands: list[str],
        instances: list[EC2Instance],
        print_output: bool = True,
    ) -> None:
        """
        This function executes a list of commands on each instance provided.
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

    def install_cluster_dependencies(self) -> None:
        """Install common dependencies on the manager and worker instances."""
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

    def install_network_instances_dependencies(self) -> None:
        """Install common dependencies on the proxy, trusted host, and gatekeeper instances."""
        commands = [
            # Update and Install Python3 and flask
            "sudo apt-get update",
            "sudo apt-get install -y python3-pip",
            "sudo pip3 install flask requests",
        ]

        # Execute commands
        self.execute_commands(
            commands,
            [self.proxy_instance, self.trusted_host_instance, self.gatekeeper_instance],
            print_output=False,
        )

    def run_sys_bench(self) -> None:
        """Run sysbench on the manager and worker instances."""
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

    def save_sys_bench_results(self) -> None:
        """Download the sysbench results from the manager and worker instances."""
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

    def upload_flask_apps_to_instances(self) -> None:
        """Upload the corresponding script (of a Flask server) to each instance."""
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

        # Upload proxy script to proxy instance
        try:
            ssh_client = create_ssh_client(
                self.proxy_instance.instance.public_ip_address,
                "ubuntu",
                self.ssh_key_path,
            )
            scp = SCPClient(ssh_client.get_transport())
            scp.put("scripts/proxy_script.py", "proxy_script.py")
            scp.put("public_ips.json", "public_ips.json")

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            scp.close()
            ssh_client.close()

        # Upload trusted host script to trusted host instance
        try:
            ssh_client = create_ssh_client(
                self.trusted_host_instance.instance.public_ip_address,
                "ubuntu",
                self.ssh_key_path,
            )
            scp = SCPClient(ssh_client.get_transport())
            scp.put("scripts/trusted_host_script.py", "trusted_host_script.py")
            scp.put("public_ips.json", "public_ips.json")

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            scp.close()
            ssh_client.close()

        # Upload gatekeeper script to gatekeeper instance
        try:
            ssh_client = create_ssh_client(
                self.gatekeeper_instance.instance.public_ip_address,
                "ubuntu",
                self.ssh_key_path,
            )
            scp = SCPClient(ssh_client.get_transport())
            scp.put("scripts/gatekeeper_script.py", "gatekeeper_script.py")
            scp.put("public_ips.json", "public_ips.json")

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            scp.close()
            ssh_client.close()

    def start_db_cluster_apps(self) -> None:
        """Start the Flask app on the manager and workers instances."""
        commands = [
            "nohup python3 manager_script.py > manager_output.log 2>&1 &",
        ]
        self.execute_commands(commands, [self.manager_instance])

        commands = [
            "nohup python3 worker_script.py > worker_output.log 2>&1 &",
        ]
        self.execute_commands(commands, self.worker_instances)

    def start_proxy_app(self) -> None:
        """Start the Flask app on the proxy instance."""
        commands = [
            "nohup python3 proxy_script.py > proxy_output.log 2>&1 &",
        ]
        self.execute_commands(commands, [self.proxy_instance])

    def start_trusted_host_app(self) -> None:
        """Start the Flask app on the trusted host instance."""
        commands = [
            "nohup python3 trusted_host_script.py > trusted_host_output.log 2>&1 &",
        ]
        self.execute_commands(commands, [self.trusted_host_instance])

    def start_gatekeeper_app(self) -> None:
        """Start the Flask app on the gatekeeper instance."""
        commands = [
            "nohup python3 gatekeeper_script.py > gatekeeper_output.log 2>&1 &",
        ]
        self.execute_commands(commands, [self.gatekeeper_instance])

    def set_mode(self, mode: str) -> None:
        # Set the mode on the proxy instance
        response = requests.post(
            f"http://{self.gatekeeper_instance.instance.public_ip_address}:5000/mode",
            json={"mode": mode},
        )
        print(response.json())

    def benchmark(self) -> dict[str, Any]:
        answers = []
        # 1000 write queries
        for i in range(1000):
            initial_time = time.time()
            response = requests.post(
                f"http://{self.gatekeeper_instance.instance.public_ip_address}:5000/query",
                json={
                    "query": "INSERT INTO actor (first_name, last_name) VALUES ('John', 'Doe')"
                },
            )
            answers.append(
                {
                    "time": time.time() - initial_time,
                    "response": response.json(),
                }
            )

        # 1000 read queries
        for i in range(1000):
            initial_time = time.time()
            response = requests.post(
                f"http://{self.gatekeeper_instance.instance.public_ip_address}:5000/query",
                json={"query": "SELECT COUNT(*) AS total_entries FROM actor;"},
            )
            answers.append(
                {
                    "time": time.time() - initial_time,
                    "response": response.json(),
                }
            )

        return answers

    def cleanup(self, all_instances: list[EC2Instance]) -> None:
        """
        Delete the target groups, terminate all instances and delete the security groups.

        """
        try:
            # Terminate EC2 instance
            instances_ids = [ec2_instance.instance.id for ec2_instance in all_instances]
            self.ec2_client.terminate_instances(InstanceIds=instances_ids)
            print(f"Termination of instances {instances_ids} initiated.")

            waiter = self.ec2_client.get_waiter("instance_terminated")
            waiter.wait(InstanceIds=instances_ids)
            print("Instances terminated.")

            # Delete security groups
            self.ec2_client.delete_security_group(
                GroupId=self.cluster_security_group_id
            )
            print(f"Security group {self.cluster_security_group_id} deleted.")

            self.ec2_client.delete_security_group(GroupId=self.proxy_security_group_id)
            print(f"Security group {self.proxy_security_group_id} deleted.")

            self.ec2_client.delete_security_group(
                GroupId=self.trusted_host_security_group_id
            )
            print(f"Security group {self.trusted_host_security_group_id} deleted.")

            self.ec2_client.delete_security_group(
                GroupId=self.gatekeeper_security_group_id
            )
            print(f"Security group {self.gatekeeper_security_group_id} deleted.")

            # Delete key pair
            self.ec2_client.delete_key_pair(KeyName=self.key_name)
            os.remove(self.ssh_key_path)

        except ClientError as e:
            print(f"An error occurred: {e}")

    def _get_latest_ubuntu_ami(self) -> str:
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

ec2_manager = EC2Manager()

# Clear data folder
os.system("rm -rf data")
os.system("mkdir data")

ec2_manager.create_key_pair()
time.sleep(5)
print("Launching instances...")
all_instances = ec2_manager.launch_instances()

# Wait for instances to be running
print("Waiting for instances to be running...")
for ec2_instance in all_instances:
    ec2_instance.instance.wait_until_running()
    ec2_instance.instance.reload()
    print(f"Instance {ec2_instance.get_name()} is running.")

print("All instances are running.")
time.sleep(10)

ec2_manager.add_inbound_rules()

# Save public ips to a JSON file
instance_data = {}
for ec2_instance in all_instances:
    instance_data[ec2_instance.name] = ec2_instance.instance.public_ip_address

with open("public_ips.json", "w") as file:
    json.dump(instance_data, file, indent=4)

print("Installing cluster dependencies...")
ec2_manager.install_cluster_dependencies()

print("Installing proxy, trusted host and gatekeeper dependencies...")
ec2_manager.install_network_instances_dependencies()

print("Running sysbench...")
ec2_manager.run_sys_bench()

print("Saving sysbench results...")
ec2_manager.save_sys_bench_results()

print("Uploading Flask apps to instances...")
ec2_manager.upload_flask_apps_to_instances()

print("Starting Flask apps for the manager and workers...")
ec2_manager.start_db_cluster_apps()

print("Starting Flask app for the proxy...")
ec2_manager.start_proxy_app()

print("Starting Flask app for the trusted host...")
ec2_manager.start_trusted_host_app()

print("Starting Flask app for the gatekeeper...")
ec2_manager.start_gatekeeper_app()

time.sleep(10)

# benchmark
while True:
    print("Benchmarking...")
    ec2_manager.set_mode("DIRECT_HIT")
    answers_direct_hit = ec2_manager.benchmark()
    with open("data/benchmark_direct_hit.json", "w") as file:
        json.dump(answers_direct_hit, file, indent=4)
    print_stats(answers_direct_hit)

    ec2_manager.set_mode("RANDOM")
    answers_random = ec2_manager.benchmark()
    with open("data/benchmark_random.json", "w") as file:
        json.dump(answers_random, file, indent=4)
    print_stats(answers_random)

    ec2_manager.set_mode("CUSTOMIZED")
    answers_customized = ec2_manager.benchmark()
    with open("data/benchmark_customized.json", "w") as file:
        json.dump(answers_customized, file, indent=4)
    print_stats(answers_customized)

    press_touched = input("Press`b` to benchmark again, any other to cleanup: ")
    if press_touched != "b":
        break

# Cleanup
ec2_manager.cleanup(all_instances)
print("Cleanup complete.")

