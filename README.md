# Charging Infrastructure Data Modelling

The goal of 


The project was the Capstone project for the Udacity Data Engineering Nanodegree Course. 


## Overview

Here you want to write a short overview of the goals of your project and how it works at a high level. If possible, include one or two images of the end product and architecture diagram (see examples below). diagrams.net is a great tool for creating architecture diagrams.

The project consist of the following steps: 
1. Data Acquisition: Calling chargecloud API in regular time intervals and storing raw results
2. Data Preprocessing: Clean data and transform API results into flat files  
3. Data Modelling and ETL process ingesting charging data into Data Warehouse (Redshift) 


### Data Architecture


If you decide to include this, you should also talk a bit about why you chose the architecture and tools you did for this project.


### Data Model

![Data Modelling](er_diagram.png)


### Data Visualization


## Prerequisites

Directions or anything needed before running the project.

- Prerequisite 1
- Prerequisite 2
- Prerequisite 3

## How to Run This Project

Replace the example step-by-step instructions with your own.

1. Install packages in `requirements.txt`
2. Run command: `python x`
3. Make sure it's running properly by checking z
4. To clean up at the end, run script: `python cleanup.py`

## Design Choices 

- ingesting raw data into Redshift allows for flexible refactoring of data modell 
- separation each step of ETL process (Data Acquisition, Data Cleaning, Data Modelling and Loading) allows some or all 
of those steps can be moved to Airflow or AWS


## Lessons Learned


It's good to reflect on what you learned throughout the process of building this project. Here you might discuss what you would have done differently if you had more time/money/data. Did you end up choosing the right tools or would you try something else next time?

## Next Steps 

Here are a few steps to improve the project, but were out of scope of the capstone project 
- deal with additional columns not yet implemented in the data model (e.g. `opening_hours` or `capabilities`)
- deal with slowly changing dimension tables (SCD)
- implement data acquisition, batch processing and Data Warehouse Update in Airflow or AWS


## Contact

Please feel free to contact me if you have any questions: 
- [linkedin](https://www.linkedin.com/in/nick-losacker/)
- [email](mailto:nick.losacker@eon.com)


# Acknowledgements 

[Data-Engineering Template by JPHaus](https://github.com/JPHaus/data-engineering-project-template)