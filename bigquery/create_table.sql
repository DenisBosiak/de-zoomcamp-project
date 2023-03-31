-- Create external table "stations"
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-project-381912.bike_sharing_data.stations`
OPTIONS (
  uris=['gs://dtc-de-project-data-lake/upload/stations.parquet'],
  format=parquet,
  hive_partition_uri_prefix='gs://dtc-de-project-data-lake/upload/stations'
);


-- Create external table "weather"
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-project-381912.bike_sharing_data.weather`
OPTIONS (
  uris=['gs://dtc-de-project-data-lake/upload/weather.parquet'],
  format=parquet,
  hive_partition_uri_prefix='gs://dtc-de-project-data-lake/upload/weather'
);


-- Create external table "trips"
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-project-381912.bike_sharing_data.trips`
OPTIONS (
  uris=['gs://dtc-de-project-data-lake/upload/trips.parquet'],
  format=parquet,
  hive_partition_uri_prefix='gs://dtc-de-project-data-lake/upload/trips'
);


-- Create external table "sharing_trips"
CREATE OR REPLACE TABLE `dtc-de-project-381912.bike_sharing_data.sharing_trips`
PARTITION BY start_date AS
SELECT * FROM `dtc-de-project-381912.bike_sharing_data.trips`