# Washington public bike sharing
## Final project for data engineering zoomcamp 2023
This project created for scope historical data by public bike sharing in the city of Washington.

## Problem Description
It was decided to analyze period from 2011 and 2020: how many trips were, what category of users was the biggest customer, did trips have dependence from precipitation. For this it was creating a data pipeline to importing, cleaning and converting data and creating a docker image for next perform the same task.

## Used Technologies
- GCP:
    - BigQuery - DWH;
    - Google Cloud Storage - Data Lake;
- Terraform - Infrascructure as a Service;
- Docker - creating an image for project;
- Prefect - orchestration tool;
- Dbt - data transformation;
- Looker Studio - data visualization.

## Dataset
The dataset downloaded from [kaggle](https://www.kaggle.com/datasets/jeanmidev/public-bike-sharing-in-north-america) community. It consist from three csv files: trips, stations and weather (from weather used only 2 columns).

**Trips:**<br>
start_date - start date of the trip<br>
start_station_code - start station of the trip<br>
end_date - end date of the trip<br>
end_station_code - end station of the trip<br>
duration_sec - duration of the trip in seconds<br>
is_member - determine if the trip has been done by a member<br>
yearid - year of trip

**Stations:**<br>
code - station code<br>
name - name of the station<br>
latitude - station latitude<br>
longitude - station longitude<br>
yearid - year of used<br>

**Weather:**<br>
date - date<br>
prectot - precipitation<br>

## Data Pipeline Schema 
![Schema](https://github.com/DenisBosiak/de-zoomcamp-project/blob/main/images/pipeline_schema.png)
## Development Steps

## Data Visualizations
Dashboard created by Looker Studio and can found [here](https://lookerstudio.google.com/reporting/d61853ad-3d05-48a9-9c89-0a4d443fc1a9).