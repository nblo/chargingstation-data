# Overview

The goal of this project is to dynamically model the occupancy of charging station in Germany. The data is gathered by with the 
help of the [ChargeCloud API](https://www.chargecloud.de/), a German E-Mobility company providing back-end solutions for Charging Point Operators. 


The project consist of the following steps: 
1. Data Acquisition: Calling Chargecloud API in regular time intervals and storing raw results
2. Data Cleaning: Postprocess API results and transform data into flat files  
3. Data Modelling and ETL process: Set up data model and ingest charging data into Data Warehouse (Redshift)


The goal of the project is to set up a maintainable and easy to extend data architecture and ETL-process for charging-data. Some example use cases for data model: 
- business reporting: generate utilization reports for single charging stations, cities or operators
- BI-dashboards: Generate interactive visualizations for visualizing charging station usage
- Predictive modelling: Use utilization data to predict usage development or determine optimal charging point locations
with Machine Learning Models

# Step 1: Data Acquisition


# Step 2: Data Cleaning 


# Step 3: Data Modelling and Ingestion


# Design Choices 

n_cp = 2423

n_conn = 2806


n_total = n_cp + n_conn

n_total * 365 * 24 * 6

- Redshift vs. PostGres 
- Batch Processing vs. Streaming. 


# Next Steps 


# Addressing other scenarios 

- Currently the the data gathered results in roughly 275 million rows a year or about 750k rows per day. 
If the data volume was to be increased by 100x (28 billion rows per year or 75 million rows per day) Redshift would still be a viable option. As the data is ingested via batch processing at the end of the day, this could still be done in a single job. However, it could be advisable to 
either ingest data more than once a day or split ingestion into smaller chunks.

If the pipeline would be run every day daily basis by 7 am every day

The database needed to be accessed by 100+ people.

# Data Model 

The data model incorporates two fact tables (status information of charging points and connectors) and 
four dimension tables (master data for charging stations, charging points, connectors and time metadata). 


![Data Model](er_diagram.png)


## Fact tables 

- `status_chargingpoints`: status of charging point

| column    name   | description           | datatype  |
| :--|:-------------|:-----|
| **id_status_cp**      |  unique identifier of chargingpoint status |varchar |
| id_chargingpoint      |  charging point id |varchar |
| query_time | timestamp of API call  |   timestamptz |
| status_cp |    status of charging point |    varchar |
| status_parkingsensor |   status of parking sensor (if available)  |    varchar |

---
- `status_connectors`: status of connectors

| column    name   | description           | datatype  |
| :--|:-------------|:-----|
| **id_status_connectors**      |  unique identifier of connector status |varchar |
| id_connector      |  connector id |varchar |
| query_time | timestamp of API call    |   timestamptz |
| status_connector |   status of connector  |    varchar |


## Dimension tables 
- `charging_station` charging station master data

| column    name   | description           | datatype  |
| :--|:-------------|:-----|
| **id_cs** |    unique identifier for charging point  |    int |
| name |   charging station name  |    varchar |
| address | charging station address (street + house number)   |    varchar |
| city |   charging station city |    varchar |
| postal_code | charging station postal code |    varchar |
| country | charging station country |    varchar |
| owner | charging station owner (if different than charging point operator) |    varchar |
| roaming | whether roaming is available |    boolean |
| longitude | charging station longitude (EPSG 4326) |    float |
| latitude | charging station latitude (EPSG 4326) |    float |
| operator_name | charging station operator (CPO) |    varchar |
| operator_hotline | charging station operator hotline |    varchar |
| open_24_7 | whether charging station is open 24/7 |    boolean |


---
- `charging_point` charging point master data

| column    name   | description           | datatype  |
| :--|:-------------|:-----|
| **id_cp** |    unique identifier for charging point  |    varchar |
| id_cs |  unique identifier to corresponding charging station  |    int |
| charging_station_position |  description specifying charging station position  |    varchar |
| cp_position |  description specifying charging point position (e.g. left or right charging point)  |    varchar |
| vehicle_type | suitable type of vehicle    |    varchar |
| floor_level |  charging point floor level  |    varchar |

---

- `connector` connector master data

| column    name   | description           | datatype  |
| :--|:-------------|:-----|
| **id_connector** |    unique identifier for connector  |    varchar |
| id_cp |   unique identifier for charging point  |    varchar |
| format |  type of plug (socket or permanently installed cable)  |    varchar |
| power_type |  type of power (AC 1-Phase, AC 3-Phase, DC)  |    varchar |
| ampere |  maximum amperage  |    int |
| voltage |  nominal voltage   |    int |
| max_power |  maximum charging power  |    int |
| standard |   charging standard (Chademo, IEC 62196) |    varchar |


---

- `time` time metadata

| column    name   | description           | datatype  |
| :--|:-------------|:-----|
| **query_time** |   time of API call |    timestamptz |
| hour |   timestamp hour of day  |    int4 |
| day |  timestamp day of month  |    int4 |
| week |   timestamp week of year|    int4 |
| month | timestamp month  |    int4 |
| year |  timestamp  year |    int4 |
| weekday |  timestamp day of week  |    int4 |

