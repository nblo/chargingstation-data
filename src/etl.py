import pandas as pd
import logging 
import configparser
import psycopg2

from sql import (drop_table_queries,
                 drop_constraints_queries, 
                 create_table_queries,
                 copy_table_queries,
                 insert_table_queries,
                 SQL_CONSTRAINTS)

logger = logging.getLogger(__name__)


def drop_tables(cur, conn):
    """Drop staging, fact and dimension tables from AWS Redshift

    Args:
        cur: AWS Redshift cursor object
        conn: AWS Redshift connection object
    """ 
    for query in drop_table_queries:
        logger.info(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()


def drop_constraints(cur, conn):
    """Drop staging, fact and dimension tables from AWS Redshift

    Args:
        cur: AWS Redshift cursor object
        conn: AWS Redshift connection object
    """ 
    existing_constraints = set(pd.read_sql(con=conn, sql=SQL_CONSTRAINTS)["constraint_name"])
    for query in drop_constraints_queries:
        constraint_name = query.split("CONSTRAINT")[1].strip()
        if constraint_name in existing_constraints: 
            logger.info(f"Executing query: {query}")
            cur.execute(query)
            conn.commit()      


def create_tables(cur, conn):
    """Create staging, fact and dimension tables in AWS Redshift

    Args:
        cur: AWS Redshift cursor object
        conn: AWS Redshift connection object
    """ 
    for query in create_table_queries:
        logger.info(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()
        

def load_staging_tables(cur, conn):
    """Load song data and log data from files in S3 into staging tables in AWS Redshift 

    Args:
        cur: AWS Redshift cursor object
        conn: AWS Redshift connection object
    """    
    for query in copy_table_queries:
        logger.info(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data into fact and dimension tables from staging tables 
    
    Args:
        cur: AWS Redshift cursor object
        conn: AWS Redshift connection object
    """    
    for query in insert_table_queries:
        logger.info(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()


def main(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    logger.info(f"Dropping tables.")
    drop_tables(cur, conn)
    
    logger.info(f"Dropping constraints.")
    drop_constraints(cur, conn)
    
    logger.info(f"Creating tables.")
    create_tables(cur, conn)
    
    logger.info(f"Loading staging tables.")
    load_staging_tables(cur, conn) 
    
    
    logger.info(f"Ingesting data from staging tables.")
    insert_tables(cur, conn)
    
    conn.close()
    

if __name__ == '__main__': 
    main()