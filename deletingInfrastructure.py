import base64
import configparser
import json
import logging
import os
import time

import boto3

from utilityFunctions import decoder, prettyRedshiftProps

logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config.sections()

logging.info("Reading the config file")
config.read(os.getcwd()+"/dwh.cfg")


user = os.environ["LOGNAME"]
secretFile = f"/Users/{user}/aws_editor.json"

with open (secretFile,"r") as secret:
    file = json.load(secret)

DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")

logging.info("Initiating and creating the redshift client")
redshift = boto3.client('redshift',
                        region_name=config.get("CLUSTER","DWH_REGION"),
                        aws_access_key_id=decoder(file['access_key']),
                        aws_secret_access_key=decoder(file['secret_key'])
                        )
logging.info("Initiating and creating the iam client")
iam = boto3.client('iam',
                  aws_access_key_id=decoder(file['access_key']),
                  aws_secret_access_key=decoder(file['secret_key']))


logging.info("Deleting the redshift cluster and skipping saving the snapshot")
try:
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  
                           SkipFinalClusterSnapshot=True)
except Exception as e:
    logging.info(e)


logging.info("Deleting the iam role and detaching the policy")
try:
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, 
                          PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
except Exception as e:
    logging.info(e)

configFilePath = os.getcwd() + "/dwh.cfg"

logging.info("Removing the config file and its contents")
if os.path.exists(configFilePath):
    os.remove(configFilePath)
