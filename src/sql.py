import configparser
from dataclasses import dataclass
import pandas as pd 
import logging
import typing 

logger = logging.getLogger(__name__)

TEMPLATE_TEST_CASE_ROW_COUNT = "select count(*) > 0 from {SCHEMA}.{TABLE_NAME}"

@dataclass
class DataTestCase:
    name: str
    sql: str
    how: str = 'all' 


@dataclass
class DataIngester: 
    table_name: str
    drop_table: str
    create_table: str
    populate_table: str
    drop_constraints: str = None
    data_test_cases: typing.List[DataTestCase] = None
 

SQL_CONSTRAINTS = """ select tco.constraint_SCHEMA,
       tco.constraint_name,
       kcu.ordinal_position as position,
       kcu.column_name as key_column,
       kcu.table_SCHEMA || '.' || kcu.table_name as table
from information_SCHEMA.table_constraints tco
join information_SCHEMA.key_column_usage kcu 
     on kcu.constraint_name = tco.constraint_name
     and kcu.constraint_SCHEMA = tco.constraint_SCHEMA
     and kcu.constraint_name = tco.constraint_name
where tco.constraint_type = 'PRIMARY KEY'
order by tco.constraint_SCHEMA,
         tco.constraint_name,
         kcu.ordinal_position;"""

DROP_TABLE_STAGING_STATUS_CHARGING_POINTS = """DROP TABLE IF EXISTS {SCHEMA}.staging_status_cp"""

CREATE_TABLE_STAGING_STATUS_CHARGING_POINTS = """ 
                             CREATE TABLE IF NOT EXISTS {SCHEMA}.staging_status_cp 
                             (
                              status_cp                  VARCHAR       NOT NULL, 
                              id_cp                      VARCHAR       NOT NULL, 
                              parkingsensor_status       VARCHAR, 
                              ts                         TIMESTAMPTZ   NOT NULL
                             )
"""

COPY_TABLE_STAGING_STATUS_CHARGING_POINTS = """COPY {SCHEMA}.staging_status_cp
                                               FROM '{STATUS_DATA_CHARGING_POINT}'
                                               CREDENTIALS 'aws_iam_role={ROLE_ARN}'
                                               REGION 'us-east-2' 
                                               DELIMITER ';' IGNOREHEADER 1;
; 
"""

DATA_TEST_CASES_STAGING_STATUS_CP = [DataTestCase(name="row_count_staging_status_cp", 
                                                  sql=TEMPLATE_TEST_CASE_ROW_COUNT)]


staging_status_charging_points = DataIngester(table_name="staging_status_cp", 
                                              drop_table=DROP_TABLE_STAGING_STATUS_CHARGING_POINTS, 
                                              create_table=CREATE_TABLE_STAGING_STATUS_CHARGING_POINTS, 
                                              populate_table=COPY_TABLE_STAGING_STATUS_CHARGING_POINTS,
                                              data_test_cases=DATA_TEST_CASES_STAGING_STATUS_CP)


DROP_TABLE_STAGING_STATUS_CONNECTORS = """DROP TABLE IF EXISTS {SCHEMA}.staging_status_connectors"""

CREATE_TABLE_STAGING_STATUS_CONNECTORS = """ 
                             CREATE TABLE IF NOT EXISTS {SCHEMA}.staging_status_connectors
                             (
                              status_connector  VARCHAR       NOT NULL, 
                              id_connector      VARCHAR       NOT NULL, 
                              ts                TIMESTAMPTZ   NOT NULL
                             )
"""

COPY_TABLE_STAGIGING_STATUS_CONNECTORS =  """COPY {SCHEMA}.staging_status_connectors
                                             FROM '{STATUS_DATA_CHARGING_CONNECTORS}'
                                             CREDENTIALS 'aws_iam_role={ROLE_ARN}'
                                             REGION 'us-east-2' 
                                             DELIMITER ';' IGNOREHEADER 1;
"""


DATA_TEST_CASES_STAGING_STATUS_CONN = [DataTestCase(name="row_count_staging_connectors", 
                                                    sql=TEMPLATE_TEST_CASE_ROW_COUNT)]


staging_status_connectors = DataIngester(table_name="staging_status_connectors", 
                                         drop_table=DROP_TABLE_STAGING_STATUS_CONNECTORS, 
                                         create_table=CREATE_TABLE_STAGING_STATUS_CONNECTORS, 
                                         populate_table=COPY_TABLE_STAGIGING_STATUS_CONNECTORS,
                                         data_test_cases=DATA_TEST_CASES_STAGING_STATUS_CONN)


DROP_TABLE_STAGING_CHARGING_STATIONS = """DROP TABLE IF EXISTS {SCHEMA}.staging_charging_stations"""

CREATE_TABLE_STAGING_CHARGING_STATIONS = """
                                CREATE TABLE IF NOT EXISTS {SCHEMA}.staging_charging_stations
                                (
                                 id                      INTEGER, 
                                 name                    VARCHAR, 
                                 address                 VARCHAR, 
                                 city                    VARCHAR, 
                                 postal_code             VARCHAR, 
                                 country                 VARCHAR,
                                 distance_in_m           FLOAT, 
                                 owner                   VARCHAR, 
                                 roaming                 BOOLEAN, 
                                 latitude                FLOAT, 
                                 longitude               FLOAT, 
                                 operator_name           VARCHAR, 
                                 operator_hotline        VARCHAR, 
                                 open_24_7               BOOLEAN
                                 )
"""

COPY_TABLE_STAGING_CHARGING_STATIONS = """COPY {SCHEMA}.staging_charging_stations
                                          FROM '{MASTER_DATA_CHARGING_STATIONS}' 
                                          CREDENTIALS 'aws_iam_role={ROLE_ARN}'
                                          REGION 'us-east-2' 
                                          DELIMITER ';' IGNOREHEADER 1
                                          EMPTYASNULL;                             
"""


staging_charging_stations = DataIngester(table_name="staging_charging_stations", 
                                         drop_table=DROP_TABLE_STAGING_CHARGING_STATIONS, 
                                         create_table=CREATE_TABLE_STAGING_CHARGING_STATIONS, 
                                         populate_table=COPY_TABLE_STAGING_CHARGING_STATIONS
                                         )

DROP_TABLE_STAGING_CHARGING_POINTS = """DROP TABLE IF EXISTS {SCHEMA}.staging_charging_points"""

CREATE_TABLE_STAGING_CHARGING_POINTS = """ 
                                CREATE TABLE IF NOT EXISTS staging_charging_points 
                                (
                                 id_cs                      INTEGER NOT NULL,
                                 charging_station_position  VARCHAR,
                                 roaming                    BOOLEAN, 
                                 physical_reference         VARCHAR, 
                                 cp_parking_space_numbers   VARCHAR,
                                 cp_position                VARCHAR, 
                                 cp_public_comment          VARCHAR, 
                                 id                         VARCHAR, 
                                 vehicle_type               VARCHAR,
                                 floor_level                VARCHAR, 
                                 uid                        INTEGER NOT NULL
                                )
"""

COPY_TABLE_STAGING_CHARGING_POINTS = """COPY {SCHEMA}.staging_charging_points 
                                        FROM '{MASTER_DATA_CHARGING_POINTS}' 
                                        CREDENTIALS 'aws_iam_role={ROLE_ARN}'
                                        REGION 'us-east-2' 
                                        DELIMITER ';' IGNOREHEADER 1 
                                        EMPTYASNULL 
"""

staging_charging_points = DataIngester(table_name="staging_charging_points", 
                                       drop_table=DROP_TABLE_STAGING_CHARGING_POINTS, 
                                       create_table=CREATE_TABLE_STAGING_CHARGING_POINTS, 
                                       populate_table=COPY_TABLE_STAGING_CHARGING_POINTS
                                       )

DROP_TABLE_STAGING_CONNECTORS = """DROP TABLE IF EXISTS {SCHEMA}.staging_connectors"""

CREATE_TABLE_STAGING_CONNECTORS = """ 
                                CREATE TABLE IF NOT EXISTS {SCHEMA}.staging_connectors 
                                (
                                 id_cp          VARCHAR NOT NULL,          
                                 format         VARCHAR,
                                 power_type     VARCHAR, 
                                 id             VARCHAR, 
                                 tariff_id      VARCHAR, 
                                 ampere         INTEGER, 
                                 max_power      INTEGER,
                                 voltage        INTEGER, 
                                 standard       VARCHAR
                                )
"""

COPY_TABLE_STAGING_CONNECTORS = """COPY {SCHEMA}.staging_connectors 
                                   FROM '{MASTER_DATA_CONNECTORS}' 
                                   CREDENTIALS 'aws_iam_role={ROLE_ARN}'
                                   REGION 'us-east-2' 
                                   DELIMITER ';' IGNOREHEADER 1;   
"""

staging_charging_connectors = DataIngester(table_name="staging_connectors", 
                                           drop_table=DROP_TABLE_STAGING_CONNECTORS, 
                                           create_table=CREATE_TABLE_STAGING_CONNECTORS, 
                                           populate_table=COPY_TABLE_STAGING_CONNECTORS
                                           )

DROP_TABLE_STATUS_CHARGING_POINTS = "DROP TABLE IF EXISTS {SCHEMA}.status_chargingpoints"

CREATE_TABLE_STATUS_CHARGING_POINTS = """CREATE TABLE IF NOT EXISTS {SCHEMA}.status_chargingpoints
                                        (
                                         id_status_cp               VARCHAR NOT NULL, 
                                         id_chargingpoint           VARCHAR NOT NULL, 
                                         query_time                 TIMESTAMPTZ NOT NULL, 
                                         status_cp                  VARCHAR, 
                                         status_parkingsensor       VARCHAR, 
                                         CONSTRAINT status_chargingpoints_pkey PRIMARY KEY (id_status_cp)
                                        ) 
"""

DROP_STATUS_CP_PKEY = "ALTER TABLE {SCHEMA}.status_chargingpoints DROP CONSTRAINT status_chargingpoints_pkey"

INSERT_TABLE_STATUS_CHARGING_POINTS = """INSERT INTO {SCHEMA}.status_chargingpoints (
                                                SELECT 
                                                    md5(id_cp || ts) id_status_cp,
                                                    id_cp as id_chargingpoint, 
                                                    ts as query_time, 
                                                    status_cp, 
                                                    parkingsensor_status as status_parkingsensor 
                                                FROM staging_status_cp)
""" 

status_chargingpoints = DataIngester(table_name="status_chargingpoints", 
                                     drop_table=DROP_TABLE_STATUS_CHARGING_POINTS, 
                                     create_table=CREATE_TABLE_STATUS_CHARGING_POINTS, 
                                     populate_table=INSERT_TABLE_STATUS_CHARGING_POINTS, 
                                     drop_constraints=DROP_STATUS_CP_PKEY
                                     )

DROP_TABLE_STATUS_CONNECTORS = "DROP TABLE IF EXISTS {SCHEMA}.status_connectors"
DROP_STATUS_CONN_PKEY = "ALTER TABLE {SCHEMA}.status_connectors DROP CONSTRAINT status_connectors_pkey"

CREATE_TABLE_STATUS_CONNECTORS = """CREATE TABLE IF NOT EXISTS {SCHEMA}.status_connectors
                                    (
                                     id_status_connector                VARCHAR NOT NULL, 
                                     id_connector                       VARCHAR NOT NULL, 
                                     query_time                         TIMESTAMPTZ NOT NULL, 
                                     status_connector                   VARCHAR, 
                                     CONSTRAINT status_connectors_pkey PRIMARY KEY (id_status_connector)
                                    ) 
"""

INSERT_TABLE_STATUS_CONNECTORS = """INSERT INTO {SCHEMA}.status_connectors (
                                                SELECT 
                                                    md5(id_connector || ts) id_status_cp,
                                                    id_connector, 
                                                    ts as query_time, 
                                                    status_connector
                                                FROM staging_status_connectors)
""" 

status_connectors = DataIngester(table_name="status_connectors", 
                                 drop_table=DROP_TABLE_STATUS_CONNECTORS, 
                                 create_table=CREATE_TABLE_STATUS_CONNECTORS, 
                                 populate_table=INSERT_TABLE_STATUS_CONNECTORS, 
                                 drop_constraints=DROP_STATUS_CONN_PKEY
                                 )


DROP_TABLE_CHARGING_STATION = "DROP TABLE IF EXISTS {SCHEMA}.charging_station"
DROP_CS_PKEY = "ALTER TABLE {SCHEMA}.charging_station DROP CONSTRAINT charging_station_pkey"


CREATE_TABLE_CHARGING_STATION = """
                                CREATE TABLE IF NOT EXISTS {SCHEMA}.charging_station
                                (
                                 id_cs                      INTEGER, 
                                 name                       VARCHAR, 
                                 address                    VARCHAR, 
                                 city                       VARCHAR, 
                                 postal_code                VARCHAR, 
                                 country                    VARCHAR,
                                 owner                      VARCHAR, 
                                 roaming                    BOOLEAN, 
                                 latitude                   FLOAT, 
                                 longitude                  FLOAT, 
                                 operator_name              VARCHAR, 
                                 operator_hotline           VARCHAR, 
                                 open_24_7                  BOOLEAN,
                                 CONSTRAINT charging_station_pkey PRIMARY KEY (id_cs)
                                 )
"""

INSERT_TABLE_CHARGING_STATION = """INSERT INTO  {SCHEMA}.charging_station (
                                                SELECT 
                                                    id as id_cs, 
                                                    name, 
                                                    address, 
                                                    city, 
                                                    postal_code, 
                                                    country, 
                                                    owner, 
                                                    roaming, 
                                                    latitude, 
                                                    longitude, 
                                                    operator_name, 
                                                    operator_hotline, 
                                                    open_24_7
                                                FROM staging_charging_stations
)
"""
charging_station = DataIngester(table_name="charging_station", 
                                drop_table=DROP_TABLE_CHARGING_STATION, 
                                create_table=CREATE_TABLE_CHARGING_STATION, 
                                populate_table=INSERT_TABLE_CHARGING_STATION, 
                                drop_constraints=DROP_CS_PKEY
                                )


DROP_TABLE_CHARGING_POINT = "DROP TABLE IF EXISTS {SCHEMA}.charging_point"
DROP_CP_PKEY = "ALTER TABLE {SCHEMA}.charging_point DROP CONSTRAINT charging_point_pkey"

CREATE_TABLE_CHARGING_POINT = """ 
                                CREATE TABLE IF NOT EXISTS {SCHEMA}.charging_point
                                (
                                 id_cp                          VARCHAR NOT NULL, 
                                 id_cs                          INTEGER NOT NULL,
                                 charging_station_position      VARCHAR,
                                 roaming                        BOOLEAN, 
                                 physical_reference             VARCHAR, 
                                 cp_parking_space_numbers       VARCHAR,
                                 cp_position                    VARCHAR, 
                                 vehicle_type                   VARCHAR,
                                 floor_level                    VARCHAR,
                                 CONSTRAINT charging_point_pkey PRIMARY KEY (id_cp)
                                )
"""


INSERT_TABLE_CHARGING_POINT = """INSERT INTO {SCHEMA}.charging_point (
                                                SELECT 
                                                    id as id_cp,
                                                    id_cs,
                                                    charging_station_position,
                                                    roaming,
                                                    physical_reference,       
                                                    cp_parking_space_numbers, 
                                                    cp_position,              
                                                    vehicle_type,             
                                                    floor_level            
                                                FROM staging_charging_points
)
"""

charging_point = DataIngester(table_name="charging_point", 
                              drop_table=DROP_TABLE_CHARGING_POINT, 
                              create_table=CREATE_TABLE_CHARGING_POINT, 
                              populate_table=INSERT_TABLE_CHARGING_POINT, 
                              drop_constraints=DROP_CP_PKEY
                              )


DROP_TABLE_CONNECTOR = "DROP TABLE IF EXISTS {SCHEMA}.connector"
DROP_CONN_PKEY = "ALTER TABLE {SCHEMA}.connector DROP CONSTRAINT connector_pkey"

CREATE_TABLE_CONNECTOR = """ 
                         CREATE TABLE IF NOT EXISTS {SCHEMA}.connector 
                        (
                         id_connector               VARCHAR NOT NULL, 
                         id_cp                      VARCHAR NOT NULL,          
                         format                     VARCHAR,
                         power_type                 VARCHAR, 
                         ampere                     INTEGER, 
                         max_power                  INTEGER,
                         voltage                    INTEGER, 
                         standard                   VARCHAR,
                         CONSTRAINT connector_pkey PRIMARY KEY (id_connector)
                        )
"""

INSERT_TABLE_CONNECTOR = """INSERT INTO {SCHEMA}.connector (
                                SELECT 
                                    id as id_connector, 
                                    id_cp as id_cp, 
                                    format, 
                                    power_type, 
                                    ampere, 
                                    max_power, 
                                    voltage, 
                                    standard
                            FROM staging_connectors
)
"""

connector = DataIngester(table_name="connector", 
                         drop_table=DROP_TABLE_CONNECTOR, 
                         create_table=CREATE_TABLE_CONNECTOR, 
                         populate_table=INSERT_TABLE_CONNECTOR, 
                         drop_constraints=DROP_CONN_PKEY
                         )


DROP_TABLE_TIME = "DROP TABLE IF EXISTS {SCHEMA}.time"
DROP_TIME_PKEY = """ALTER TABLE {SCHEMA}."time" DROP CONSTRAINT time_pkey"""

CREATE_TABLE_TIME = """CREATE TABLE IF NOT EXISTS {SCHEMA}."time" (
                        query_time                  timestamptz NOT NULL,
                        "hour"                      int4,
	                    "day"                       int4,
	                    week                        int4,
	                    "month"                     int4,
	                    "year"                      int4,
	                    weekday                     int4,
	                    CONSTRAINT time_pkey PRIMARY KEY (query_time)
                     )
"""

INSERT_TABLE_TIME  = """ INSERT INTO {SCHEMA}."time"  (  
                                    SELECT 
                                        ts as query_time, 
                                        min(extract(hour from ts)) as  "hour",
                                        min(extract(day from ts)) as "day",
                                        min(extract(week from ts)) as week, 
                                        min(extract(month from ts)) as "month",
                                        min(extract(year from ts)) as "year",
                                        min(extract(dayofweek from ts)) as weekday
                            FROM staging_status_cp 
                            GROUP BY query_time
)
"""

time = DataIngester(table_name="time", 
                    drop_table=DROP_TABLE_TIME, 
                    create_table=CREATE_TABLE_TIME, 
                    populate_table=INSERT_TABLE_TIME, 
                    drop_constraints=DROP_TIME_PKEY
                    )

data_objects_staging = [staging_charging_connectors, 
                        staging_charging_points,
                        staging_charging_stations, 
                        staging_status_charging_points, 
                        staging_status_connectors]

data_objects = [status_chargingpoints, 
                status_connectors, 
                charging_station,
                charging_point,
                connector 
                ]

drop_table_queries = [DROP_TABLE_STAGING_STATUS_CHARGING_POINTS,
                      DROP_TABLE_STAGING_STATUS_CONNECTORS,
                      DROP_TABLE_STAGING_CHARGING_STATIONS, 
                      DROP_TABLE_STAGING_CHARGING_POINTS, 
                      DROP_TABLE_STAGING_CONNECTORS, 
                      DROP_TABLE_STATUS_CHARGING_POINTS, 
                      DROP_TABLE_STATUS_CONNECTORS, 
                      DROP_TABLE_CHARGING_STATION, 
                      DROP_TABLE_CHARGING_POINT, 
                      DROP_TABLE_CONNECTOR, 
                      DROP_TABLE_TIME]

drop_constraints_queries = [DROP_CONN_PKEY, 
                            DROP_CP_PKEY, 
                            DROP_CS_PKEY, 
                            DROP_STATUS_CONN_PKEY, 
                            DROP_STATUS_CP_PKEY, 
                            DROP_TIME_PKEY]

create_table_queries = [CREATE_TABLE_STAGING_CHARGING_POINTS, 
                        CREATE_TABLE_STAGING_STATUS_CONNECTORS, 
                        CREATE_TABLE_STAGING_STATUS_CHARGING_POINTS,
                        CREATE_TABLE_STAGING_CHARGING_STATIONS, 
                        CREATE_TABLE_STAGING_CHARGING_POINTS, 
                        CREATE_TABLE_STAGING_CONNECTORS, 
                        CREATE_TABLE_STATUS_CHARGING_POINTS, 
                        CREATE_TABLE_STATUS_CONNECTORS, 
                        CREATE_TABLE_CHARGING_STATION, 
                        CREATE_TABLE_CHARGING_POINT, 
                        CREATE_TABLE_CONNECTOR, 
                        CREATE_TABLE_TIME]

copy_table_queries = [COPY_TABLE_STAGING_STATUS_CHARGING_POINTS, 
                      COPY_TABLE_STAGIGING_STATUS_CONNECTORS, 
                      COPY_TABLE_STAGING_CHARGING_STATIONS, 
                      COPY_TABLE_STAGING_CHARGING_POINTS, 
                      COPY_TABLE_STAGING_CONNECTORS]


insert_table_queries = [INSERT_TABLE_STATUS_CHARGING_POINTS, 
                        INSERT_TABLE_STATUS_CONNECTORS,
                        INSERT_TABLE_CONNECTOR, 
                        INSERT_TABLE_CHARGING_STATION, 
                        INSERT_TABLE_CHARGING_POINT, 
                        INSERT_TABLE_TIME]