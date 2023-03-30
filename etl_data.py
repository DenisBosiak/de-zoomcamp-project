import os
import glob
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


dataset_name = "public-bike-sharing-in-north-america"
local_folder = "dataset"
city_name = 'washington'


@task()
def download_dataset(folder: str, dataset: str) -> None :
    """Download dataset locally"""
    bash_command = f"cd {folder} && kaggle datasets download -d jeanmidev/{dataset}"
    os.system(bash_command)
    return


@task()
def unzip_file(folder: str, dataset: str, city: str) -> list :
    """Unzip file into local directory"""
    #For ubuntu
    #bash_command = f"cd {folder} && unzip {dataset}.zip" ubuntu
    bash_command = f"cd {folder} && tar -xf {dataset}.zip"
    #os.system(bash_command)
    #files_list = os.listdir(f"{folder}/{city}")
    files_list = []
    path = Path(f"{folder}/{city}")
    for file in path.rglob("*"):
        files_list.append(f"{file}")
    return files_list


@task()
def clean_directory() -> None :
    """Delete unnecessary files and directories"""
    bash_command = f"cd {local_folder} && rm -r {city_name} && rm *.zip"
    os.system(bash_command)
    return        


@task(log_prints=True)
def convert_to_parquet(file_list: list) -> None :
    """Convert csv file into parquet"""
    for file in file_list:
        df = pd.read_csv(f"{local_folder}/{city}/{file}", nrows=1000)

        if file == 'trips':
            df.start_date = pd.to_datetime(df.start_date)
            df.end_date = pd.to_datetime(df.end_date)
        
        if file == 'weather':
            df.date = pd.to_datetime(df.date)

        trips_table = pa.Table.from_pandas(df)
        pq_file_location = f"dataset/{city}/{file}"
        pq.write_table(trips_table, pq_file_location.replace("csv", "parquet"))
    return


@task()
def upload_to_gcs(file_name) -> None:
    bucket = GcsBucket.load("de-bucket")
    #bucket.upload_from_path(from_path=)
    return


@flow()
def etl_flow():
    #download_dataset(local_folder, dataset_name)
    file_list = unzip_file(local_folder, dataset_name, city_name)
    #convert_to_parquet(file_list)
    print(file_list)
    path = Path(f"{local_folder}/{city_name}")
    print(path)


if __name__ == "__main__":
    etl_flow()