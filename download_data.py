import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from google.cloud import storage
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
"""

dataset_name = "public-bike-sharing-in-north-america"
local_folder = "dataset"
path_to_key = './dtc-de-project-381912-6d5626791f2b.json'
client = storage.Client.from_service_account_json(json_credentials_path=path_to_key)



def convert_to_parquet(file_name) -> None :
    df = pd.read_csv(f"dataset/washington/{file_name}.csv", nrows=1000)

    if file_name == 'trips':
        df.start_date = pd.to_datetime(df.start_date)
        df.end_date = pd.to_datetime(df.end_date)
    
    if file_name == 'weather':
        df.date = pd.to_datetime(df.date)

    trips_table = pa.Table.from_pandas(df)
    pq.write_table(trips_table, f"dataset/bike_sharing/{file_name}.parquet")


def upload_to_gcs(file_name) -> None:
    bucket = storage.Bucket(client, 'dtc-de-project-data-lake')
    blob = bucket.blob(f"{file_name}.parquet")
    blob.upload_from_filename(f"dataset/bike_sharing/{file_name}.parquet")



if __name__ == '__main__':
    convert_to_parquet('stations')
    #convert_to_parquet('trips')
    #convert_to_parquet('weather')
    upload_to_gcs('stations')



"""
# FOR DAG
 
download_dataset = BashOperator(
    task_id = "download_dataset",
    bash_command = f"cd {local_folder} && kaggle datasets download -d jeanmidev/{dataset_name}"
    )

unzip_file = BashOperator(
    task_id = "unzip_file",
    bash_command = f"cd {local_folder} && unzip {dataset_name}.zip"
    )

clean_directory = BashOperator(
    task_id = "clean_directory",
    bash_command = f"cd {local_folder} && rm -r washington && rm *.zip"
)
"""