import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


dataset_name = "public-bike-sharing-in-north-america"
dataset_folder = 'dataset'
city_name = 'washington'
folder_to_upload = 'upload'


@task()
def download_dataset(folder: str, dataset: str) -> None :
    """Download dataset locally"""
    bash_command = f"cd {folder} && kaggle datasets download -d jeanmidev/{dataset}"
    os.system(bash_command)
    return


@task()
def unzip_file(folder: str, dataset: str, city: str) -> list :
    """Unzip file into local directory"""
    bash_command = f"cd {folder} && tar -xf {dataset}.zip"
    os.system(bash_command)
    files_list = os.listdir(f"{folder}/{city}")  
    return files_list


@task()
def clean_directory(folder: str) -> None :
    """Delete unnecessary files and directories"""
    bash_command = f"cd {folder} && del *.zip"
    os.system(bash_command)
    return        


@task(log_prints=True)
def convert_to_parquet(folder: str, file: str, folder_parquet: str) -> str :
    """Convert csv file into parquet and clean some columns"""
    df = pd.read_csv(f"{folder}/{file}")

    if file == 'trips':
        df.start_date = pd.to_datetime(df.start_date)
        df.end_date = pd.to_datetime(df.end_date)
        
    if file == 'weather':
        df.date = pd.to_datetime(df.date)

    trips_table = pa.Table.from_pandas(df)
    pq_file_name = file.replace("csv", "parquet")
    pq_file_location = f"{folder_parquet}/{pq_file_name}"
    pq.write_table(trips_table, pq_file_location)
    return pq_file_location


@task()
def upload_to_gcs(path: str) -> None:
    """Upload local parquet file to GCS"""
    bucket = GcsBucket.load("de-bucket")
    bucket.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_flow():
    """Main flow"""
    download_dataset(dataset_folder, dataset_name)
    file_list = unzip_file(dataset_folder, dataset_name, city_name)
    for file in file_list:
        file_path = convert_to_parquet(f"{dataset_folder}/{city_name}", file, folder_to_upload)
        print(file_path)
        
        upload_to_gcs(file_path)
    clean_directory(dataset_folder)


if __name__ == "__main__":
    etl_flow()