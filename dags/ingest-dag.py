import os
import logging
from datetime import datetime, date
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.json as pj
import pyarrow.parquet as pq
# import pandas as pd

# ENV Variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NFL = os.environ.get("GCP_GCS_BUCKET_NFL")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

# File Handling Variables
# Airflow docs on date macros for formatting
# https://airflow.apache.org/docs/apache-airflow/1.10.12/macros-ref.html#airflow.macros.ds_format

year = '{{ macros.ds_format(ds, \'%Y-%m-%d\' ,\'%Y\') }}'
nfl_dataset_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/28/schedule?season={year}"
nfl_source_file = f"nfl_season_{year}.json"
nfl_parquet_file = nfl_source_file.replace('.json', '.parquet')
nfl_gcs_object_path = f"raw/schedule/{nfl_parquet_file}"

# Functions

def format_to_parquet(src_file):
  if not src_file.endswith('json'):
        logging.error("Can only accept source files in json format, for the moment")
        return
  print("COMMENCE PARQUETIZATION...")
  table = pj.read_json(src_file)
  pq.write_table(table, src_file.replace('.json', '.parquet'))
  print(src_file)

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    print("UPLOADING TO GCS...")
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def download_parquetize_upload_gcs_operators(
    dag,
    dataset_url,
    source_file,
    parquet_file,
    gcs_object_path

):
  with dag:
    download_data_task = BashOperator(
       task_id="download_data_task",
       bash_command=f"echo DOWNLOADING DATA... && \
        curl -sSLf {dataset_url} > {AIRFLOW_HOME}/{source_file} && \
        ls {AIRFLOW_HOME}"
    )

    parquetize_data_task = PythonOperator(
       task_id="parquetize_data_task",
       python_callable=format_to_parquet,
       op_kwargs={
          "src_file": f"{AIRFLOW_HOME}/{source_file}"
       }
    )

    local_to_gcs_task = PythonOperator(
       task_id="local_to_gcs_task",
       python_callable=upload_to_gcs,
       op_kwargs={
          "bucket": BUCKET_NFL,
          "object_name": f"{gcs_object_path}",
          "local_file": f"{AIRFLOW_HOME}/{parquet_file}"
       }
    )

    download_data_task >> parquetize_data_task >> local_to_gcs_task

default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}

nfl_ingest_dag = DAG(
  dag_id="nfl_ingest_dag",
  default_args=default_args,
  schedule_interval = "@yearly",
  start_date=datetime(1999, 1, 1),
  # catchup will kick off a dag run for any data interval that has not been run since the last data interval
  catchup=True,
  # try to limit active concurrent runs or machine could freak out
  max_active_runs=3,
  tags=['nfl_ingest_dag']
)

download_parquetize_upload_gcs_operators(
    dag=nfl_ingest_dag,
    dataset_url=nfl_dataset_url,
    source_file=nfl_source_file,
    parquet_file=nfl_parquet_file,
    gcs_object_path=nfl_gcs_object_path
)