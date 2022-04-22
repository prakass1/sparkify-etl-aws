import boto3
import click
from dataclasses import dataclass
import configparser
import json
from time import sleep
from botocore.exceptions import ClientError



@dataclass
class Environments:
    key: str
    secret: str
    region: str
    cluster_type: str
    n_nodes: str
    node_type: str
    iam_role_name: str
    cluster_identifier: str
    db_name: str
    db_user: str
    db_password: str
    db_port: str


def init():
    # CONFIG
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    envs = Environments(
        key=config.get("CLUSTER", "ACCESS_KEY"),
        secret=config.get("CLUSTER", "SECRET"),
        region=config.get("CLUSTER", "REGION"),
        cluster_type=config.get("CLUSTER", "DWH_CLUSTER_TYPE"),
        n_nodes=config.get("CLUSTER", "DWH_NUM_NODES"),
        node_type=config.get("CLUSTER", "DWH_NODE_TYPE"),
        iam_role_name=config.get("CLUSTER", "DWH_IAM_ROLE_NAME"),
        cluster_identifier=config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER"),
        db_name=config.get("CLUSTER", "DB_NAME"),
        db_user=config.get("CLUSTER", "DB_USER"),
        db_password=config.get("CLUSTER", "DB_PASSWORD"),
        db_port=config.get("CLUSTER", "DB_PORT"),
    ).__dict__
    if None in envs.values() or "" in envs.values():
        return -1
    return envs


def attach_sg(ec2, props, envs):
    try:
        vpc = ec2.Vpc(id=props["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(envs["db_port"]),
            ToPort=int(envs["db_port"]),
        )
    except ClientError as exc:
        print(exc)
        print("Continuing...")
    except Exception as e:
        print(e)


def create_cluster(ec2, redshift, envs):
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=envs["cluster_type"],
            NodeType=envs["node_type"],
            NumberOfNodes=int(envs["n_nodes"]),
            # Identifiers & Credentials
            DBName=envs["db_name"],
            ClusterIdentifier=envs["cluster_identifier"],
            MasterUsername=envs["db_user"],
            MasterUserPassword=envs["db_password"],
            # Roles (for s3 access)
            IamRoles=[envs["roleARN"]],
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            props = redshift.describe_clusters(
                ClusterIdentifier=envs["cluster_identifier"]
            )["Clusters"][0]
            cluster_status = props["ClusterStatus"].lower()
            while cluster_status != "available":
                print("Cluster is still being prepared ...")
                print(f"The current status is {cluster_status}")
                props = redshift.describe_clusters(
                    ClusterIdentifier=envs["cluster_identifier"]
                )["Clusters"][0]
                cluster_status = props["ClusterStatus"].lower()
                sleep(30)

            props = redshift.describe_clusters(
                ClusterIdentifier=envs["cluster_identifier"]
            )["Clusters"][0]
            print(
                f"The cluster is available with the status is {props['ClusterStatus']}"
            )
            end_point_port = (
                f"{props['Endpoint']['Address']}:{props['Endpoint']['Port']}"
            )
            role_arn = props["IamRoles"][0]["IamRoleArn"]
            print(f"The created endpoint with port is {end_point_port}")
            endpoint_address = props["Endpoint"]["Address"]
            print(
                f"Use the Host - {endpoint_address} in the ETL process by updating the dwh.cfg"
            )
            print(f"The role arn created is {role_arn}")
            # Updating the envs
            envs["endpoint_address"] = endpoint_address
            # Allow VPC to connect to the db_port. If exists then simply print the exception.
            attach_sg(ec2, props, envs)
        else:
            print(
                f"The status of creation is not success and code returned is {response['ResponseMetadata']['HTTPStatusCode']}"
            )
            print("Response")
            print(response)
    except ClientError as exc:
        print(exc)
    except Exception as e:
        raise e


def teardown_cluster(redshift, envs):
    try:
        response = redshift.delete_cluster(
            ClusterIdentifier=envs["cluster_identifier"], SkipFinalClusterSnapshot=True
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            props = redshift.describe_clusters(
                ClusterIdentifier=envs["cluster_identifier"]
            )["Clusters"][0]
            cluster_status = props["ClusterStatus"]
            while cluster_status == "deleting":
                print("Cluster is still being deleted ...")
                print(f"The current status is {cluster_status}")
                props = redshift.describe_clusters(
                    ClusterIdentifier=envs["cluster_identifier"]
                )["Clusters"][0]
                cluster_status = props["ClusterStatus"]
                sleep(10)
        else:
            print("Something has gone wrong")
            print("Response:")
            print(response)
    except redshift.exceptions.ClusterNotFoundFault as exc:
        print("The Cluster does not exist anymore... You can create it again")
        raise exc
    except redshift.exceptions.InvalidClusterStateFault as e:
        raise e


def create_iam_role(iam, envs):
    try:
        response = iam.create_role(
            Path="/",
            RoleName=envs["iam_role_name"],
            Description="Allows Redshift clusters to call AWS services on your behalf",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except iam.exceptions.EntityAlreadyExistsException as exc:
        print(exc)
        print("Continuing...")
    except Exception as e:
        print(e)


@click.command()
@click.option(
    "--name",
    prompt="Your options either to create/delete/delete_role for creation of cluster, and deletion.",
    help="This is used to create redshift cluster or delete the cluster",
)
def do_work(name):
    # init the environment variables
    try:
        envs = init()
        if envs != -1:
            # Based on name either create or destroy
            iam = boto3.client(
                "iam",
                region_name=envs["region"],
                aws_access_key_id=envs["key"],
                aws_secret_access_key=envs["secret"],
            )
            redshift = boto3.client(
                "redshift",
                region_name=envs["region"],
                aws_access_key_id=envs["key"],
                aws_secret_access_key=envs["secret"],
            )
            ec2 = boto3.resource(
                "ec2",
                region_name=envs["region"],
                aws_access_key_id=envs["key"],
                aws_secret_access_key=envs["secret"],
            )

            if name == "create":
                # Create a iam role.
                print("Creating a new IAM Role")
                resp = create_iam_role(iam, envs)
                print("Attaching Policy")
                policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                response = iam.attach_role_policy(
                    PolicyArn=policy_arn, RoleName=envs["iam_role_name"]
                )
                print(response["ResponseMetadata"]["HTTPStatusCode"])
                assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
                print("Get the IAM role ARN")
                roleArn = iam.get_role(RoleName=envs["iam_role_name"])["Role"]["Arn"]
                print(f"The role arn is {roleArn}, attaching that to envs")
                envs["roleARN"] = roleArn
                # Creation of redshift cluster
                create_cluster(ec2, redshift, envs)
                # Print the total environment information.
                print(
                    "The cluster and environment details are listed down below to be used in the environment files"
                )
                for k, v in envs.items():
                    print(f"||{k}||{v}||")

            elif name == "delete":
                # Destroy reshift cluster and associated entities.
                teardown_cluster(redshift, envs)
            elif name == "delete_role":
                # remove associated role information.
                r1 = iam.detach_role_policy(
                    RoleName=envs["iam_role_name"],
                    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
                )
                r2 = iam.delete_role(RoleName=envs["iam_role_name"])
                if (
                    not (
                        r1["ResponseMetadata"]["HTTPStatusCode"]
                        or r2["ResponseMetadata"]["HTTPStatusCode"]
                    )
                    == 200
                ):
                    print("Something went wrong while deleting the role policy")
                else:
                    print("The role and policy are deleted")
        else:
            print(
                "The required envs cannot be empty in the env file for creation of resources in the AWS"
            )
    except Exception as e:
        print(e)


if __name__ == "__main__":
    do_work()
