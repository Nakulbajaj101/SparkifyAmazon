import configparser

config = configparser.ConfigParser()

config["CLUSTER"] = {
'DB_NAME':"dwh",
'DB_USER':'dwh_user',
'DB_PASSWORD':'dwhPassword008',
'DB_PORT':'5439',
'DWH_REGION':"us-west-2",
'DWH_CLUSTER_TYPE':"multi-node",
'DWH_NUM_NODES':"4",
'DWH_NODE_TYPE':"dc2.large",
'DWH_CLUSTER_IDENTIFIER':"dwhCluster",
'DWH_IAM_ROLE_NAME':"dwhRole"
}

config["S3"] = {
'LOG_DATA':"s3://udacity-dend/log_data",
'LOG_JSONPATH':"s3://udacity-dend/log_json_path.json",
'SONG_DATA':"s3://udacity-dend/song_data",
'REGION':"us-west-2"
}

with open("dwh.cfg", 'w') as configfile:
    config.write(configfile)
