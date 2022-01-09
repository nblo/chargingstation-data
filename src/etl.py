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


def _execute_query(query_fmt: str, mapping_fmt_query: dict, cur: cursor, conn: connection = None):    
    query = query_fmt.format(**mapping_fmt_query)
    logger.debug(f" Executing query '{query:}'")
    cur.execute(query)
    
    if conn: 
        conn.commit()


def _perform_data_unit_test(data_test_cases: typing.List[DataTestCase], 
                            conn: connection,
                            mapping_fmt_query: dict,
                            table_name: str): 
    """[summary]

    Args:
        data_test_cases (typing.List[DataTestCase]): [description]
        conn (connection): [description]
        mapping_fmt_query (dict): [description]
        table_name (str): [description]

    Raises:
        ValueError: [description]
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
            err_msg = f"Data quality check '{dtc.name}' failed. SQL statement '{dtc.sql}' evaluated to False."
            logger.error(err_msg)
            raise ValueError(err_msg)
        
    logger.info(f"All data quality checks passed for table '{table_name}'.")


def ingest_data(data_obj: DataIngester, cur: cursor, conn: connection, mapping_fmt_queries: dict): 
    """[summary]

    Args:
        data_obj (DataIngester): [description]
        cur (cursor): [description]
        conn (connection): [description]
        mapping_fmt_queries (dict): [description]
    """      
    # dropping table 
    if data_obj.drop_table: 
        logger.info(f"Dropping table '{data_obj.table_name}'.") 
        _execute_query(data_obj.drop_table, cur=cur, mapping_fmt_query=mapping_fmt_queries)
    
    # dropping constraints 
    if data_obj.drop_constraints: 
        existing_constraints = set(pd.read_sql(con=conn, sql=SQL_CONSTRAINTS)["constraint_name"])
        constraint_name = data_obj.drop_constraints.split("CONSTRAINT")[1].strip()
        
        if constraint_name in existing_constraints: 
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
        _perform_data_unit_test(data_obj.data_test_cases, 
                                conn=conn,
                                table_name=data_obj.table_name, 
                                mapping_fmt_query=mapping_fmt_queries)
    

def _create_data_model(cur: cursor,
                       conn: connection,
                       data_objs: typing.List[DataIngester], 
                       mapping_fmt_queries: dict): 
    """[summary]

    Args:
        cur (cursor): [description]
        conn (connection): [description]
        data_objs (typing.List[DataIngester]): [description]
        mapping_fmt_queries (dict): [description]
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
    
    logger.info(f"Ingesting data into  tables.")
    for di in data_ingestions:
        _create_data_model(cur=cur, conn=conn, data_objs=di, mapping_fmt_queries=mapping_fmt_queries)
    
    conn.close()
    

if __name__ == '__main__': 
    logging.config.dictConfig(LOGGING_CONFIG)
    main(config_file=CONFIG_FILE)