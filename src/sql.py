import configparser
import dataclasses

# CONFIG
config = configparser.ConfigParser()
config.read('../config.cfg')

SQL_CONSTRAINTS = """ select tco.constraint_schema,
       tco.constraint_name,
       kcu.ordinal_position as position,
       kcu.column_name as key_column,
       kcu.table_schema || '.' || kcu.table_name as table
from information_schema.table_constraints tco
join information_schema.key_column_usage kcu 
     on kcu.constraint_name = tco.constraint_name
     and kcu.constraint_schema = tco.constraint_schema
     and kcu.constraint_name = tco.constraint_name
where tco.constraint_type = 'PRIMARY KEY'
order by tco.constraint_schema,
         tco.constraint_name,
         kcu.ordinal_position;"""

DROP_TABLE_STAGING_STATUS_CHARGING_POINTS = """DROP TABLE IF EXISTS staging_status_cp"""

CREATE_TABLE_STAGING_STATUS_CHARGING_POINTS = """ 
                             CREATE TABLE IF NOT EXISTS staging_status_cp 
                             (
                              id_cp                      VARCHAR       NOT NULL, 
                              status_cp                  TEXT          NOT NULL, 
                              parkingsensor_status       TEXT, 
                              ts                         TIMESTAMPTZ   NOT NULL
                             )
"""

COPY_TABLE_STAGING_STATUS_CHARGING_POINTS = """COPY staging_status_cp
                                               FROM '{STATUS_DATA_CHARGING_POINT}'
                                               CREDENTIALS 'aws_iam_role={ROLE_ARN}'
                                               REGION 'us-east-2' 
                                               DELIMITER ';' IGNOREHEADER 1;
; 
""".format(STATUS_DATA_CHARGING_POINT=config["S3"]["STATUS_DATA_CHARGING_POINT"], 
           ROLE_ARN=config["IAM_ROLE"]["ARN"])


DROP_TABLE_STAGING_STATUS_CONNECTORS = """DROP TABLE IF EXISTS staging_status_connectors"""

CREATE_TABLE_STAGING_STATUS_CONNECTORS = """ 
                             CREATE TABLE IF NOT EXISTS staging_status_connectors
                             (
                              id_connector              VARCHAR       NOT NULL, 
                              status_connector          TEXT          NOT NULL, 
                              ts                        TIMESTAMPTZ   NOT NULL
                             )
"""

COPY_TABLE_STAGIGING_STATUS_CONNECTORS =  """COPY staging_status_connectors
                                             FROM '{STATUS_DATA_CHARGING_CONNECTORS}'
                                             CREDENTIALS 'aws_iam_role={ROLE_ARN}'
                                             REGION 'us-east-2' 
                                             DELIMITER ';' IGNOREHEADER 1;
""".format(STATUS_DATA_CHARGING_CONNECTORS=config["S3"]["STATUS_DATA_CHARGING_CONNECTORS"], 
           ROLE_ARN=config["IAM_ROLE"]["ARN"])


DROP_TABLE_STAGING_CHARGING_STATIONS = """DROP TABLE IF EXISTS staging_charging_stations"""

CREATE_TABLE_STAGING_CHARGING_STATIONS = """
                                CREATE TABLE IF NOT EXISTS staging_charging_stations
                                (
                                 id                      INTEGER, 
                                 name                    TEXT, 
                                 address                 TEXT, 
                                 city                    TEXT, 
                                 postal_code             TEXT, 
                                 country                 TEXT,
                                 distance_in_m           FLOAT, 
                                 owner                   TEXT, 
                                 roaming                 BOOLEAN, 
                                 latitude                FLOAT, 
                                 longitude               FLOAT, 
                                 operator_name           TEXT, 
                                 operator_hotline        TEXT, 
                                 open_24_7               BOOLEAN
                                 )
"""

COPY_TABLE_STAGING_CHARGING_STATIONS = """COPY staging_charging_stations
                                          FROM '{MASTER_DATA_CHARGING_STATIONS}' 
                                          CREDENTIALS 'aws_iam_role={ROLE_ARN}'
                                          REGION 'us-east-2' 
                                          DELIMITER ';' IGNOREHEADER 1
                                          EMPTYASNULL;                             
""".format(MASTER_DATA_CHARGING_STATIONS=config["S3"]["MASTER_DATA_CHARGING_STATIONS"], 
           ROLE_ARN=config["IAM_ROLE"]["ARN"])


DROP_TABLE_STAGING_CHARGING_POINTS = """DROP TABLE IF EXISTS staging_charging_points"""

CREATE_TABLE_STAGING_CHARGING_POINTS = """ 
                                CREATE TABLE IF NOT EXISTS staging_charging_points 
                                (
                                 id_cs                      INTEGER NOT NULL,
                                 charging_station_position  TEXT,
                                 roaming                    BOOLEAN, 
                                 physical_reference         TEXT, 
                                 cp_parking_space_numbers   TEXT,
                                 cp_position                TEXT, 
                                 cp_public_comment          TEXT, 
                                 id                         TEXT, 
                                 vehicle_type               TEXT,
                                 floor_level                TEXT, 
                                 uid                        INTEGER NOT NULL
                                )
"""

COPY_TABLE_STAGING_CHARGING_POINTS = """COPY staging_charging_points 
                                        FROM '{MASTER_DATA_CHARGING_POINTS}' 
                                        CREDENTIALS 'aws_iam_role={ROLE_ARN}'
                                        REGION 'us-east-2' 
                                        DELIMITER ';' IGNOREHEADER 1 
                                        EMPTYASNULL 
""".format(MASTER_DATA_CHARGING_POINTS=config["S3"]["MASTER_DATA_CHARGING_POINTS"], 
           ROLE_ARN=config["IAM_ROLE"]["ARN"])

DROP_TABLE_STAGING_CONNECTORS = """DROP TABLE IF EXISTS staging_connectors"""

CREATE_TABLE_STAGING_CONNECTORS = """ 
                                CREATE TABLE IF NOT EXISTS staging_connectors 
                                (
                                 id_cp          TEXT NOT NULL,          
                                 format         TEXT,
                                 power_type     TEXT, 
                                 id             TEXT, 
                                 tariff_id      TEXT, 
                                 ampere         INTEGER, 
                                 max_power      INTEGER,
                                 voltage        INTEGER, 
                                 standard       TEXT
                                )
"""

COPY_TABLE_STAGING_CONNECTORS = """COPY staging_connectors FROM '{MASTER_DATA_CONNECTORS}' 
                                   CREDENTIALS 'aws_iam_role={ROLE_ARN}'
                                   REGION 'us-east-2' 
                                   DELIMITER ';' IGNOREHEADER 1;   
""".format(MASTER_DATA_CONNECTORS=config["S3"]["MASTER_DATA_CONNECTORS"], 
           ROLE_ARN=config["IAM_ROLE"]["ARN"])


DROP_TABLE_STATUS_CHARGING_POINTS = "DROP TABLE IF EXISTS status_chargingpoints"

CREATE_TABLE_STATUS_CHARGING_POINTS = """CREATE TABLE IF NOT EXISTS status_chargingpoints
                                        (
                                         id_status_cp               VARCHAR NOT NULL, 
                                         id_chargingpoint           VARCHAR NOT NULL, 
                                         query_time                 TIMESTAMPTZ NOT NULL, 
                                         status_cp                  VARCHAR, 
                                         status_parkingsensor       VARCHAR, 
                                         CONSTRAINT status_chargingpoints_pkey PRIMARY KEY (id_status_cp)
                                        ) 
"""

DROP_STATUS_CP_PKEY = "ALTER TABLE public.status_chargingpoints DROP CONSTRAINT status_chargingpoints_pkey"

INSERT_TABLE_STATUS_CHARGING_POINTS = """INSERT INTO status_chargingpoints (
                                                SELECT 
                                                    md5(id_cp || ts) id_status_cp,
                                                    id_cp as id_chargingpoint, 
                                                    ts as query_time, 
                                                    status_cp, 
                                                    parkingsensor_status as status_parkingsensor 
                                                FROM staging_status_cp)
""" 


DROP_TABLE_STATUS_CONNECTORS = "DROP TABLE IF EXISTS status_connectors"
DROP_STATUS_CONN_PKEY = "ALTER TABLE public.status_connectors DROP CONSTRAINT status_connectors_pkey"

CREATE_TABLE_STATUS_CONNECTORS = """CREATE TABLE IF NOT EXISTS status_connectors
                                    (
                                     id_status_connector                VARCHAR NOT NULL, 
                                     id_connector                       VARCHAR NOT NULL, 
                                     query_time                         TIMESTAMPTZ NOT NULL, 
                                     status_connector                   VARCHAR, 
                                     CONSTRAINT status_connectors_pkey PRIMARY KEY (id_status_connector)
                                    ) 
"""

INSERT_TABLE_STATUS_CONNECTORS = """INSERT INTO status_connectors (
                                                SELECT 
                                                    md5(id_connector || ts) id_status_cp,
                                                    id_connector, 
                                                    ts as query_time, 
                                                    status_connector
                                                FROM staging_status_connectors)
""" 


DROP_TABLE_CHARGING_STATION = "DROP TABLE IF EXISTS charging_station"
DROP_CS_PKEY = "ALTER TABLE public.charging_station DROP CONSTRAINT charging_station_pkey"


CREATE_TABLE_CHARGING_STATION = """
                                CREATE TABLE IF NOT EXISTS charging_station
                                (
                                 id_cs                      INTEGER, 
                                 name                       TEXT, 
                                 address                    TEXT, 
                                 city                       TEXT, 
                                 postal_code                TEXT, 
                                 country                    TEXT,
                                 distance_in_m              FLOAT, 
                                 owner                      TEXT, 
                                 roaming                    BOOLEAN, 
                                 latitude                   FLOAT, 
                                 longitude                  FLOAT, 
                                 operator_name              TEXT, 
                                 operator_hotline           TEXT, 
                                 open_24_7                  BOOLEAN,
                                 CONSTRAINT charging_station_pkey PRIMARY KEY (id_cs)
                                 )
"""



INSERT_TABLE_CHARGING_STATION = """INSERT INTO  charging_station (
                                                SELECT 
                                                    id as id_cs, 
                                                    name, 
                                                    address, 
                                                    city, 
                                                    postal_code, 
                                                    country, 
                                                    distance_in_m, 
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

DROP_TABLE_CHARGING_POINT = "DROP TABLE IF EXISTS charging_point"
DROP_CP_PKEY = "ALTER TABLE public.charging_point DROP CONSTRAINT charging_point_pkey"

CREATE_TABLE_CHARGING_POINT = """ 
                                CREATE TABLE IF NOT EXISTS charging_point
                                (
                                 id_cp                          TEXT NOT NULL, 
                                 id_cs                          INTEGER NOT NULL,
                                 charging_station_position      TEXT,
                                 roaming                        BOOLEAN, 
                                 physical_reference             TEXT, 
                                 cp_parking_space_numbers       TEXT,
                                 cp_position                    TEXT, 
                                 cp_public_comment              TEXT, 
                                 vehicle_type                   TEXT,
                                 floor_level                    TEXT,
                                 CONSTRAINT charging_point_pkey PRIMARY KEY (id_cp)
                                )
"""


INSERT_TABLE_CHARGING_POINT = """INSERT INTO  charging_point (
                                                SELECT 
                                                    id as id_cp,
                                                    id_cs,
                                                    charging_station_position,
                                                    roaming,
                                                    physical_reference,       
                                                    cp_parking_space_numbers, 
                                                    cp_position,              
                                                    cp_public_comment,        
                                                    vehicle_type,             
                                                    floor_level            
                                                FROM staging_charging_points
)
"""

DROP_TABLE_CONNECTOR = "DROP TABLE IF EXISTS connector"
DROP_CONN_PKEY = "ALTER TABLE public.connector DROP CONSTRAINT connector_pkey"

CREATE_TABLE_CONNECTOR = """ 
                         CREATE TABLE IF NOT EXISTS connector 
                        (
                         id_connector               TEXT, 
                         id_cp                      TEXT NOT NULL,          
                         format                     TEXT,
                         power_type                 TEXT, 
                         tariff_id                  TEXT, 
                         ampere                     INTEGER, 
                         max_power                  INTEGER,
                         voltage                    INTEGER, 
                         standard                   TEXT,
                         CONSTRAINT connector_pkey PRIMARY KEY (id_connector)
                        )
"""

INSERT_TABLE_CONNECTOR = """INSERT INTO connector (
                                SELECT 
                                    id as id_connector, 
                                    id_cp as id_cp, 
                                    format, 
                                    power_type, 
                                    tariff_id, 
                                    ampere, 
                                    max_power, 
                                    voltage, 
                                    standard
                            FROM staging_connectors
)
"""

DROP_TABLE_TIME = "DROP TABLE IF EXISTS time"
DROP_TIME_PKEY = """ALTER TABLE public."time" DROP CONSTRAINT time_pkey"""

CREATE_TABLE_TIME = """CREATE TABLE IF NOT EXISTS "time" (
                        query_time                  timestamptz NOT NULL,
                        "hour"                      int4,
	                    "day"                       int4,
	                    week                        int4,
	                    "month"                     varchar(256),
	                    "year"                      int4,
	                    weekday                     varchar(256),
	                    CONSTRAINT time_pkey PRIMARY KEY (query_time)
                     )
"""

INSERT_TABLE_TIME  = """ INSERT INTO "time"  (  
                                    SELECT 
                                        ts as query_time, 
                                        extract(hour from ts),
                                        extract(day from ts),
                                        extract(week from ts), 
                                        extract(month from ts),
                                        extract(year from ts),
                                        extract(dayofweek from ts)
                            FROM staging_status_cp 
)
"""



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
                        INSERT_TABLE_CONNECTOR, 
                        INSERT_TABLE_CHARGING_STATION, 
                        INSERT_TABLE_CHARGING_POINT, 
                        INSERT_TABLE_TIME]