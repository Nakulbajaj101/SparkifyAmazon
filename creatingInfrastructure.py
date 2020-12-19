import base64
import configparser
import json
import logging
import os
import time

import boto3
import pandas as pd

from utilityFunctions import decoder, prettyRedshiftProps

logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config.sections()

logging.info("Reading the config file")
config.read(os.getcwd()+"/dwh.cfg")

user = os.environ["LOGNAME"]

# Define the path for the aws_editor secret
secretFile = f"/Users/{user}/aws_editor.json"

with open (secretFile,"r") as secret:
    file = json.load(secret)

DWH_CLUSTER_TYPE       = config.get("CLUSTER","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("CLUSTER","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("CLUSTER","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_PORT               = config.get("CLUSTER","DB_PORT")

DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")

logging.info("Initiating and creating the redshift client")
redshift = boto3.client('redshift',
                        region_name=config.get("CLUSTER","DWH_REGION"),
                        aws_access_key_id=decoder(file['access_key']),
                        aws_secret_access_key=decoder(file['secret_key'])
                        )
logging.info("Initiating the ec2 resource")
ec2 = boto3.resource('ec2',
                  region_name=config.get("CLUSTER","DWH_REGION"),
                  aws_access_key_id=decoder(file['access_key']),
                  aws_secret_access_key=decoder(file['secret_key'])
                ) 
logging.info("Initiating the s3 resource")
s3 = boto3.resource('s3',
                 region_name=config.get("S3","REGION"),
                 aws_access_key_id=decoder(file['access_key']),
                 aws_secret_access_key=decoder(file['secret_key'])
                )           

logging.info("Initiating and creating the iam client")
iam = boto3.client('iam',
                  aws_access_key_id=decoder(file['access_key']),
                  aws_secret_access_key=decoder(file['secret_key'])
                )
########## Create infrastructure

#Create IM role
try:
    logging.info('Creating a new IAM Role')
    dwhRole = iam.create_role(
    Path='/',
    RoleName=DWH_IAM_ROLE_NAME,
    AssumeRolePolicyDocument=json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": [
                                    "redshift.amazonaws.com"
                                ]
                            },
                            "Action": [
                                "sts:AssumeRole"
                            ]
                        }
                    ]
                }),
    Description='Allows Redshift clusters to call AWS services'
    )

except Exception as e:
    logging.info(e)


logging.info('Attaching Policy')

dwhRolePolicy = iam.attach_role_policy(
    RoleName=DWH_IAM_ROLE_NAME,
    PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
)

# TODO: Get and print the IAM role and policy ARN

policyArn = iam.get_policy(PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
logging.info(policyArn["Policy"]["Arn"])

roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
logging.info(roleArn)



#Creating the redshift cluster

try:
    redshift_cluster = redshift.create_cluster(
    DBName=DWH_DB,
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
    ClusterType=DWH_CLUSTER_TYPE,
    NodeType=DWH_NODE_TYPE,
    MasterUsername=DWH_DB_USER,
    MasterUserPassword=DWH_DB_PASSWORD,
    Port=int(DWH_PORT),
    AllowVersionUpgrade=True,
    NumberOfNodes=int(DWH_NUM_NODES),
    PubliclyAccessible=True,
    IamRoles=[roleArn]
)
except Exception as e:
    logging.info(e)


#Describe the cluster created

logging.info("Giving aws 3 mins to create the cluster, so can see cluster details")
time.sleep(180)
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
logging.info(prettyRedshiftProps(myClusterProps))

DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
logging.info(f"DWH_ENDPOINT :: {DWH_ENDPOINT}")
logging.info(f"DWH_ROLE_ARN :: {DWH_ROLE_ARN}")


#Update config file
config = configparser.ConfigParser()

config.add_section("DWH_EXTRAS")
config.set("DWH_EXTRAS","DWH_ENDPOINT",DWH_ENDPOINT)
config.set("DWH_EXTRAS","DWH_ROLE_ARN",DWH_ROLE_ARN)

with open(os.getcwd()+"/dwh.cfg","a+") as configfile:
    configfile.readline()
    config.write(configfile)


#Opening a tcp port access to the 

try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    logging.info(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    logging.info(e)
