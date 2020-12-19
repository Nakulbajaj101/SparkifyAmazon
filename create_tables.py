import configparser
import logging

import psycopg2

from sql_queries import (create_schema_queries, create_table_queries,
                         drop_schema_queries, drop_table_queries)

from utilityFunctions import execute_queries

# setting up logging
logging.basicConfig(level=logging.INFO)

def drop_tables(cur, conn):
    """
    Executes the drop tables queries and drops
    the tables passed in drop_table_queries
    """

    logging.info("Running drop table queries")
    execute_queries(cur, conn, drop_table_queries)

def drop_schema(cur, conn):
    """
    Executes the drop schema queruies and drops 
    the schemas passed in drop_schema_queries
    """

    logging.info("Running drop schema queries")
    execute_queries(cur, conn, drop_schema_queries)

def create_schema(cur, conn):
    """
    Executes the create schemas queries and creates
    schemas passed in create_schema_queries
    """

    logging.info("Running create schema queries")
    execute_queries(cur, conn, create_schema_queries)

def create_tables(cur, conn):
    """
    Executes the create tables queries and creates
    tables passed in the create_table_queries
    """

    logging.info("Running create table queries")
    execute_queries(cur, conn, create_table_queries)

def main():
    """
    Wrapper function that reads the config file, creates the connection
    and executes all the queries to prepare database (schema) and tables 
    on the redshift cluster.
    """

    config = configparser.ConfigParser()

    # reading the config file
    config.read('dwh.cfg')

    DWH_DB=config.get("CLUSTER","DB_NAME")
    DWH_DB_USER=config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD=config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT=config.get("CLUSTER","DB_PORT")
    DWH_ENDPOINT=config.get("DWH_EXTRAS","DWH_ENDPOINT")

    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, 
                                                    DWH_DB_PASSWORD, 
                                                    DWH_ENDPOINT, 
                                                    DWH_PORT, 
                                                    DWH_DB)

    logging.info(f"creating the connection to redshift cluster {DWH_DB}")
    
    try:
        # creating the connection
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        # running the create schema in the first line, when connecting 
        # to the cluster first time else erroe will be raised when 
        # dropping tables since the database doesnt exist

        logging.info("Deleting the database and tables")
        create_schema(cur, conn)
        drop_tables(cur, conn)
        drop_schema(cur, conn)
        
        logging.info("Creating the database and tables")
        create_schema(cur, conn)
        create_tables(cur, conn)
        
        # closing the connection
        conn.close()
    
    except psycopg2.Error as err:
        logging.error(err)

if __name__ == "__main__":
    main()
