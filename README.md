# Washington public bike sharing
## Final project for data engineering zoomcamp 2023
This project was created for scope historical data by public bike sharing in the city of Washington.
It was decided to analyze period from 2011 and 2020.

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
The dataset was downloaded from [kaggle](https://www.kaggle.com/datasets/jeanmidev/public-bike-sharing-in-north-america) community. It consist from three csv files: trips, stations and weather.
    * Trips
    * Stations
    * Weather

## Data Pipeline Schema 