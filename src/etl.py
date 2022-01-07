import pandas as pd
import logging 
import configparser
import psycopg2
import typing

import logging.config
from settings import LOGGING_CONFIG



from sql import (DataTestCase,
                 DataIngester,
                 data_objects_staging, 
                 data_objects,
                 SQL_CONSTRAINTS)


logger = logging.getLogger(__name__)
CONFIG_FILE = "../config.cfg"


def _execute_query(query_fmt: str, mapping_fmt_query, cur, conn = None): 
    
    query = query_fmt.format(**mapping_fmt_query)
    logger.info(f"Dropping table with query '{query:}'")
    cur.execute(query)
    
    if conn: 
        conn.commit()


def _perform_data_unit_test(data_test_cases: typing.List[DataTestCase], 
                            cur,
                            mapping_fmt_query: dict,
                            table_name: str): 
    
    _mapping_fmt_query = {**mapping_fmt_query, "TABLE_NAME": table_name}
    
    for dtc in data_test_cases: 
        logger.debug(f"Running data test case: '{dtc.name}'")
        query = dtc.sql.format(**_mapping_fmt_query)
        logger.debug(f"Running sql data test case: {dtc.sql}'")
        records = pd.read_sql(con=cur, sql=query)
            
        logger.debug(f"Retrieved records of shape: '{records.shape}'.")
        if records.shape[0] == 0: 
            err_msg =  f"Data quality check failed. No result was returned."
            logger.error(err_msg)
            raise ValueError(err_msg)
            
        # first row of records contains the data unit test
        data_unit_test = records[0]
        
        # either all or any values must evaluate to True 
        _how = dtc.how
        if _how(data_unit_test):
            logger.info(f"Data quality check '{dtc.name}' passed.")
        else: 
            err_msg = f"Data quality check '{dtc.name}' failed. SQL statement '{dtc.sql}' evaluated to False."
            logger.error(err_msg)
            raise ValueError(err_msg)
        
    logger.info(f"All data quality checks passed for table '{table_name}'.")


def ingest_data(data_obj: DataIngester, cur, conn, mapping_fmt_queries: dict): 
        
        # dropping table 
        if data_obj.drop_table: 
            _execute_query(data_obj.drop_table, cur=cur, mapping_fmt_query=mapping_fmt_queries)
        
        # dropping constraints 
        if data_obj.drop_constraints: 
            existing_constraints = set(pd.read_sql(con=conn, sql=SQL_CONSTRAINTS)["constraint_name"])
            constraint_name = data_obj.drop_constraints.split("CONSTRAINT")[1].strip()
            
            if constraint_name in existing_constraints: 
                _execute_query(data_obj.drop_constraints, cur=cur, conn=conn, mapping_fmt_query=mapping_fmt_queries)

        # creating table 
        if data_obj.create_table: 
            _execute_query(data_obj.create_table, cur=cur, conn=conn, mapping_fmt_query=mapping_fmt_queries)

        # populating table 
        if data_obj.populate_table: 
            _execute_query(data_obj.populate_table, cur=cur, conn=conn, mapping_fmt_query=mapping_fmt_queries)

        conn.commit()
        
        # data quality checks
        if data_obj.data_test_cases: 
            _perform_data_unit_test(data_obj.data_test_cases, cur=cur, table_name=data_obj.table_name)
        

def _create_data_model(cur, conn, data_objs: typing.List[DataIngester], mapping_fmt_queries: dict): 
    
    for dobj in data_objs: 
        logger.info(f"Ingesting data into table '{dobj.table_name}'")
        ingest_data(dobj, cur=cur, conn=conn, mapping_fmt_queries=mapping_fmt_queries)


def main(config_file, data_objects=data_objects):
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
                           "MASTER_DATA_CONNECTORS":  config["S3"]["MASTER_DATA_CONNECTORS"]
                           }
    
    logger.info(f"Ingesting data into staging tables.")
    _create_data_model(cur=cur, conn=conn, data_objs=data_objects_staging, mapping_fmt_queries=mapping_fmt_queries)
    
    logger.info(f"Ingesting data into data model.")
    _create_data_model(cur=cur, conn=conn, data_objs=data_objects, mapping_fmt_queries=mapping_fmt_queries)
    
    conn.close()
    

if __name__ == '__main__': 
    logging.config.dictConfig(LOGGING_CONFIG)
    main(config_file=CONFIG_FILE)