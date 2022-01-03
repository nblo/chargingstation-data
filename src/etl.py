
import logging 

from sql import drop_table_queries, create_table_queries, copy_table_queries, insert_table_queries

logger = logging.getLogger(__name__)


def drop_tables(cur, conn):
    """Drop staging, fact and dimension tables from AWS Redshift

    Args:
        cur: AWS Redshift cursor object
        conn: AWS Redshift connection object
    """ 
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create staging, fact and dimension tables in AWS Redshift

    Args:
        cur: AWS Redshift cursor object
        conn: AWS Redshift connection object
    """ 
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        

def load_staging_tables(cur, conn):
    """Load song data and log data from files in S3 into staging tables in AWS Redshift 

    Args:
        cur: AWS Redshift cursor object
        conn: AWS Redshift connection object
    """    
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data into fact and dimension tables from staging tables 
    
    Args:
        cur: AWS Redshift cursor object
        conn: AWS Redshift connection object
    """    
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()