import pandas as pd
import logging 
import configparser
import psycopg2
from psycopg2.extensions import cursor, connection
import typing

import logging.config
from settings import LOGGING_CONFIG

from sql import (DataTestCase,
                 DataIngester,
                 data_ingestions_main, 
                 data_ingestions_staging,
                 SQL_CONSTRAINTS)


logger = logging.getLogger(__name__)
CONFIG_FILE = "../config.cfg"


def _execute_query(query_template: str, 
                   mapping_fmt_query: typing.List[DataTestCase], 
                   cur: cursor,
                   conn: connection = None): 
    """Execute SQL query template. 

    Args:
        query_template (str): SQL query template
        mapping_fmt_query (typing.List[DataTestCase]): mapping containing values for SQL query template placeholders.
        cur (cursor): Redshift cursor object
        conn (connection, optional): Redshift connection object. If not specified transaction is not committed. Defaults to None.
    """       
    query = query_template.format(**mapping_fmt_query)
    logger.debug(f" Executing query '{query:}'")
    cur.execute(query)
    
    if conn: 
        conn.commit()


def _run_data_unit_test(data_test_cases: typing.List[DataTestCase], 
                        conn: connection,
                        mapping_fmt_query: typing.Dict[str, str],
                        table_name: str): 
    """Execute specified data unit tests.

    Args:
        data_test_cases (typing.List[DataTestCase]): collection of data unit test to evaluate
        conn (connection): Redshift connection object.
        mapping_fmt_query (typing.Dict[str, str]): mapping containing values for SQL query template placeholders.
        table_name (str): name of table to perform data unit test on

    Raises:
        ValueError: Raised if data unit test fails. 
    """    
    _mapping_fmt_query = {**mapping_fmt_query, "TABLE_NAME": table_name}
    
    for dtc in data_test_cases: 
        logger.debug(f"Running data test case: '{dtc.name}'")
        query = dtc.sql.format(**_mapping_fmt_query)
        logger.debug(f"Running sql data test case: {dtc.sql}'")
        records = pd.read_sql(con=conn, sql=query)
            
        logger.debug(f"Retrieved records of shape: '{records.shape}'.")
        if records.shape[0] == 0: 
            err_msg =  f"Data quality check failed. No result was returned."
            logger.error(err_msg)
            raise ValueError(err_msg)
            
        # either all or any values must evaluate to True 
        _how = any if dtc.how == 'any' else all
        if _how(records.iloc[:, 0].to_list()):
            logger.info(f"Data quality check '{dtc.name}' passed.")
        else: 
            breakpoint()
            err_msg = f"Data quality check '{dtc.name}' failed. SQL statement '{query}' evaluated to False."
            logger.error(err_msg)
            raise ValueError(err_msg)
        
    logger.info(f"All data quality checks passed for table '{table_name}'.")


def ingest_data(data_obj: DataIngester, cur: cursor, conn: connection, mapping_fmt_queries: typing.Dict[str, str]): 
    """Ingest data model into redshift by dropping table and constraints, creating table, populating table with data and running data unit tests.  

    Args:
        data_obj (DataIngester): data object specifying ingestion process
        cur (cursor): Redshift cursor object
        conn (connection): Redshift connection object
        mapping_fmt_query (typing.Dict[str, str]): mapping containing values for SQL query template placeholders.
    """      
    # dropping table 
    if data_obj.drop_table: 
        logger.info(f"Dropping table '{data_obj.table_name}'.") 
        _execute_query(data_obj.drop_table, cur=cur, mapping_fmt_query=mapping_fmt_queries)
    # dropping constraints 
    if data_obj.drop_constraints: 
        schema = mapping_fmt_queries["SCHEMA"]
        existing_constraints = (pd.read_sql(con=conn, sql=SQL_CONSTRAINTS)
                                .query(f"constraint_schema=='{schema}'")
                                ["constraint_name"]
                                )
        constraint_name = data_obj.drop_constraints.split("CONSTRAINT")[1].strip()
        
        if constraint_name in set(existing_constraints): 
            logger.info(f"Dropping constraints in table '{data_obj.table_name}'.") 
            _execute_query(data_obj.drop_constraints, cur=cur, conn=conn, mapping_fmt_query=mapping_fmt_queries)

    # creating table 
    if data_obj.create_table:
        logger.info(f"Creating table '{data_obj.table_name}'.") 
        _execute_query(data_obj.create_table, cur=cur, conn=conn, mapping_fmt_query=mapping_fmt_queries)

    # populating table 
    if data_obj.populate_table: 
        logger.info(f"Populating table '{data_obj.table_name}' with records.") 
        _execute_query(data_obj.populate_table, cur=cur, conn=conn, mapping_fmt_query=mapping_fmt_queries)

    conn.commit()
    
    # data quality checks
    if data_obj.data_test_cases: 
        _run_data_unit_test(data_obj.data_test_cases, 
                            conn=conn,
                            table_name=data_obj.table_name, 
                            mapping_fmt_query=mapping_fmt_queries)
    

def _create_data_model(cur: cursor,
                       conn: connection,
                       data_objs: typing.List[DataIngester], 
                       mapping_fmt_queries: typing.Dict[str, str]): 
    """Create Redshift data model, populating tables with records and running data quality checks

    Args:
        cur (cursor): Redshift cursor object
        conn (connection): Redshift connection objects
        data_objs (typing.List[DataIngester]): sequence of data object specifying ingestion process
        mapping_fmt_query (typing.Dict[str, str]): mapping containing values for SQL query template placeholders.
    """    
    for dobj in data_objs: 
        logger.info(f"Ingesting data into table '{dobj.table_name}'")
        ingest_data(dobj, cur=cur, conn=conn, mapping_fmt_queries=mapping_fmt_queries)


def main(config_file: str, 
         data_ingestions: typing.List[typing.List[DataIngester]] = [data_ingestions_staging, data_ingestions_main]):
    logging.config.dictConfig(LOGGING_CONFIG)

    config = configparser.ConfigParser()
    config.read(config_file)

    logger.info("Connecting to redshift database...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logger.info("Successfully connected to redshift database...")


    mapping_fmt_queries = {"SCHEMA": config["CLUSTER"]["DB_SCHEMA"], 
                           "ROLE_ARN": config["IAM_ROLE"]["ARN"], 
                           "STATUS_DATA_CHARGING_POINT": config["S3"]["STATUS_DATA_CHARGING_POINT"], 
                           "STATUS_DATA_CHARGING_CONNECTORS": config["S3"]["STATUS_DATA_CHARGING_CONNECTORS"],
                           "MASTER_DATA_CHARGING_STATIONS":  config["S3"]["MASTER_DATA_CHARGING_STATIONS"], 
                           "MASTER_DATA_CHARGING_POINTS":  config["S3"]["MASTER_DATA_CHARGING_POINTS"], 
                           "MASTER_DATA_CONNECTORS":  config["S3"]["MASTER_DATA_CONNECTORS"], 
                           "MASTER_DATA_CONNECTORS":  config["S3"]["MASTER_DATA_CONNECTORS"], 
                           "MAPPING_POI_CS": config["S3"]["MAPPING_POI_CS"], 
                           "SHAPEFILE_POI_POINTS": config["S3"]["SHAPEFILE_POI_POINTS"], 
                           "SHAPEFILE_POI_POLYGONS": config["S3"]["SHAPEFILE_POI_POLYGONS"], 
                           "SHAPEFILE_POI_MULTIPOLYGONS": config["S3"]["SHAPEFILE_POI_MULTIPOLYGONS"]
                           }
    
    logger.info("Create schema if it does not exist")
    query_create_schema = 'CREATE SCHEMA IF NOT EXISTS {SCHEMA}'
    _execute_query(query_create_schema, cur=cur, conn=conn, mapping_fmt_query=mapping_fmt_queries)

    logger.info(f"Ingesting data into tables.")
    for di in data_ingestions:
        _create_data_model(cur=cur, conn=conn, data_objs=di, mapping_fmt_queries=mapping_fmt_queries)
    
    conn.close()
    

if __name__ == '__main__': 
    logging.config.dictConfig(LOGGING_CONFIG)
    main(config_file=CONFIG_FILE)