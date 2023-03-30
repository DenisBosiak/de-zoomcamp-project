-- Create external table "stations"
CREATE EXTERNAL TABLE `dtc-de-project-381912.bike_sharing_data.stations`
OPTIONS (
  uris=['gs://dtc-de-project-data-lake/upload/stations.parquet'],
  format=parquet,
  hive_partition_uri_prefix='gs://dtc-de-project-data-lake/upload/stations'
);


-- Create external table "weather"
CREATE EXTERNAL TABLE `dtc-de-project-381912.bike_sharing_data.weather`
OPTIONS (
  uris=['gs://dtc-de-project-data-lake/upload/weather.parquet'],
  format=parquet,
  hive_partition_uri_prefix='gs://dtc-de-project-data-lake/upload/weather'
);


-- Create external table "trips"
CREATE EXTERNAL TABLE `dtc-de-project-381912.bike_sharing_data.trips`
WITH PARTITION COLUMNS (
  yearid int64
)
OPTIONS (
  uris=['gs://dtc-de-project-data-lake/upload/trips.parquet'],
  format=parquet,
  hive_partition_uri_prefix='gs://dtc-de-project-data-lake/upload/trips'
);
