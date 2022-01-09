# Overview

The goal of this project is to dynamically model the occupancy of charging station in Germany. The data is gathered by with the 
help of the [chargecloud API](https://www.chargecloud.de/), a German E-Mobility company providing back-end solutions for Charging Point Operators (CPOs). 

Additionally, to analyze relationship between utilization rate and the charging stations surroundings OpenStreetMap Points-of-Interest locations
(e.g. food retailers, shopping malls, highway services) is included in the data model.  

The project consist of the following steps: 
1. Data Acquisition: 
    - Call chargecloud API in regular time intervals and storing raw results
    - Retrieve OSM data for the cities where charging stations are placed 
2. Data Cleaning: 
    - Postprocess API results and transform data into flat files of master data and utilization data
    - Match POI locations to charging station locations by spatial distance mapping between the two location datasets
3. Data Modelling and ETL process: Set up data model and ingest charging data and POI data into Data Warehouse (Redshift)


The goal of the project is to set up a maintainable and easy to extend data architecture and ETL-process for charging-data. Some example use cases for data model: 
- business reporting: generate utilization reports for single charging stations, cities or operators
- BI-dashboards: Generate interactive visualizations for visualizing charging station usage
- Predictive modelling: Use utilization data to predict usage development or determine optimal charging point locations
with Machine Learning Models

## Basics

The project contains data of electric vehicle charging stations. Here are a few basics to get familiar with charging infrastructure for electric vehicles. 

A *charging station (abbrev. cs)* (red box) is a piece of infrastructure at a single location where electric vehicles` batteries can be recharged. 

Each charging station consists of one or more *charging points (abbrev. cp)* (blue boxes), where a single electric vehicle can recharge at any given time.

Each charging point has one or more *connectors (abbrev. conn)* (green boxes) in order to satisfy different charging standards (e.g. Chademo, CCS, Type 2) or varying charging power levels (e.g. normal charging, fast-charging, ultra-fast charging). 

<img src="chargingstation.png" alt="Charging Station" width="400"/>

---
Each charging station with its charging points and connectors has static/semi-static master data. Examples of master data are the charging stations' location, address, operator, or the connectors maximum power level or connector type. 


Each chargingpoint and connector also has dynamic occupancy or status data, e.g. if the chargingpoint is occupied, reserved, 
free or out of order. 

The combination of static and dynamic data is used by car infotainment systems and apps to navigate the user to the nearest  
free and functional charging station. 

# Step 1: Data Acquisition
Script `data_acquistion.py`
#TODO


# Step 2: Data Cleaning 
Script `preprocess_results.py`
#TODO


# Step 3: Data Modelling and Ingestion
Data Modeling: `sql.py` Data Ingestion: `etl.py` 
#TODO



# Design Choices 

n_cp = 2423

n_conn = 2806


n_total = n_cp + n_conn

n_total * 365 * 24 * 6

- Redshift vs. PostGres 
- Batch Processing vs. Streaming. 


# Next Steps 

Here are a few steps to improve the project, but were out of scope of the capstone project 
- create charging events table for computing number of charging events, charging station availability or approximate energy transfer
- deal with additional columns not yet implemented in the data model (e.g. `opening_hours` or `capabilities`)
- deal with slowly changing dimension tables (SCD)
- implement data acquisition, batch processing and ETL in Airflow or AWS
- consider advantages and disadvantages of streaming data instead of batch processing
- develop a dashboard based on data model 
- expand the number of POI categories considered


# Addressing other scenarios 

- Currently the the data gathered results in roughly 275 million rows a year or about 750k rows per day. 
If the data volume was to be increased by 100x (28 billion rows per year or 75 million rows per day) Redshift would still be a viable option. As the data is ingested via batch processing at the end of the day, this could still be done in a single job. However, it could be advisable to 
either ingest data more than once a day or split ingestion into smaller chunks.

If the pipeline would be run on a daily basis by 7 am every day
- If the pipeline would be run on a daily basis I would consider Airflow as an orchestration tool. 
There could be different DAGs for ingestion jobs on different scheduling intervals: 
    - chargecloud API call (e.g. every 10 minutes)
    - batch data ingestion into status and master data tables (once or twice a day)
    - batch data ingestion of OSM data (once or twice a month)

The database needed to be accessed by 100+ people.
#TODO: 

# Data Model 

The data model incorporates two fact tables (status information of charging points and connectors) and 
six dimension tables. Four dimension tables contain master data of the charging stations and its equipment (charging stations, charging points, connectors and time metadata) and two contain POI location information (POI table and mapping table between charging stations and poi)

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
| **id_cs** |    unique identifier for charging station  |    int |
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


---

- `poi` OSM POI locations

| column    name   | description           | datatype  |
| :--|:-------------|:-----|
| **id_poi** |   OSM id (combination of osm-type and osm id) |    varchar |
| geom |   POI geometry |    geometry |
| longitude |  POI longitude (EPSG 4326)  |    float |
| latitude |   POI latitude (EPSG 4326)    |    float |
| poi_category |   timestamp week of year|    varchar |

---

- `mapping_poi_cs` Mapping table between POIs and charging stations based on distance matching

| column    name   | description           | datatype  |
| :--|:-------------|:-----|
| id_poi|   OSM id (combination of osm-type and osm id) |    varchar |
| id_cs |   unique identifier for charging station  |    int |
