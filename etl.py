import configparser
import logging

import psycopg2

from sql_queries import copy_table_queries, insert_table_queries
from utilityFunctions import execute_queries


def load_staging_tables(cur, conn):
    """
    Loads the data into the staging tables
    defined in the copy_table_queries
    """
    
    execute_queries(cur, conn, copy_table_queries)


def insert_tables(cur, conn):
    """
    Inserts the data from staging tables
    into the tables defined in the insert_table_queries
    """

    execute_queries(cur, conn, insert_table_queries)

def main():
    """
    Wrapper function that reads the config file, creates the connection
    and executes all the queries to get data into staging tables and move
    from staging tables into the analytical tables
    """

    config=configparser.ConfigParser()
    
    # reading the config file
    config.read('dwh.cfg')

    DWH_DB=config.get("CLUSTER","DB_NAME")
    DWH_DB_USER=config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD=config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT=config.get("CLUSTER","DB_PORT")
    DWH_ENDPOINT=config.get("DWH_EXTRAS","DWH_ENDPOINT")

    # creating the connection
    conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, 
                                                    DWH_DB_PASSWORD, 
                                                    DWH_ENDPOINT, 
                                                    DWH_PORT, 
                                                    DWH_DB)

    try:
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        logging.info("loading data into staging tables")
        load_staging_tables(cur, conn)

        logging.info("inserting data into tables from staging tables")
        insert_tables(cur, conn)

        # closing the connection
        conn.close()
    
    except psycopg2.Error as err:
        logging.error(err)


if __name__ == "__main__":
    main()
