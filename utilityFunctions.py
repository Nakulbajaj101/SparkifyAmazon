import base64
import logging

import pandas as pd
import psycopg2

# setting up logging
logging.basicConfig(level=logging.INFO)

def execute_queries(cur, conn, queries=[]):
    """
    Executes list of queries passed
    """

    for query in queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as err:
            logging.error(err)

def decoder(key=""):
    """
    Decodes the base64 string key
    """

    base64_string = key
    base64_bytes = base64_string.encode("ascii") 

    string_bytes = base64.b64decode(base64_bytes) 
    decoded_string = string_bytes.decode("ascii")
    return decoded_string

def prettyRedshiftProps(props):
    """provides the details of the redshift cluster in a 
    tabular format"""

    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])
