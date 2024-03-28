import os
import sys
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pyarrow.json as pj
import pyarrow.csv as pv
import pyarrow.parquet as pq

# local modules
sys.path.append('/opt/airflow/')
from jobs.ingest.nfl import ingest_nfl

# Top Level Variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Airflow docs on date macros for formatting
# https://airflow.apache.org/docs/apache-airflow/1.10.12/macros-ref.html#airflow.macros.ds_format
year = '{{ macros.ds_format(ds, \'%Y-%m-%d\' ,\'%Y\') }}'
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

#TODO modularize DAG file - move ingest / preprocessing tasks to be handled by spark jobs
# Functions
def format_to_parquet(src_file):
  print(f"SRC FILE HERE...{src_file}")
  if not (src_file.endswith('.json') or src_file.endswith('.csv')):
        logging.error("Can only accept source files in json / csv format, for the moment")
        return
  print("COMMENCE PARQUETIZATION...")
  if src_file.endswith('.json'):
    table = pj.read_json(src_file)
    pq.write_table(table, src_file.replace('.json', '.parquet'))
  elif src_file.endswith('.csv'):
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))
     
  print(src_file)

def upload_to_gcs(bucket, object_name, file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    print(f"UPLOADING TO GCS...{file}")
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(file)

def download_parquetize_upload_gcs_tasks(
    dag,
    source_file,
    parquet_file,
    bucket_name,
    gcs_object_path,
    project_id,
    bq_dataset_id,
    bq_table_id

):
#   download_data_task = BashOperator(
#     dag=dag,
#     task_id="download_data_task",
#     env={"dag_id": f"{dag.dag_id}"},
#     bash_command=f"echo DOWNLOADING DATA... && \
#     if [[ $dag_id == nfl_ingest_dag ]]; then \
#       echo RUNNING NFL INGEST...; \
#       python3 {AIRFLOW_HOME}/jobs/ingest/nfl.py; \
#     elif [[ $dag_id == elections_ingest_dag ]]; then \
#       echo RUNNING ELECTIONS INGEST...; \
#       python3 {AIRFLOW_HOME}/jobs/ingest/elections.py; \
#     else \
#       echo NO DAG HERE; \
#     fi && \
#     ls {AIRFLOW_HOME}"
# )
  download_data_task = PythonOperator(
    dag=dag,
    task_id="download_data_task",
    python_callable=ingest_nfl,
    provide_context=True
  )

  parquetize_data_task = PythonOperator(
    dag=dag,
    task_id="parquetize_data_task",
    python_callable=format_to_parquet,
    op_kwargs={
      "src_file": f"{AIRFLOW_HOME}/{source_file}"
    }
  )

  local_to_gcs_task = PythonOperator(
    dag=dag,
    task_id="local_to_gcs_task",
    python_callable=upload_to_gcs,
    op_kwargs={
      "bucket": f"{bucket_name}",
      "object_name": f"{gcs_object_path}",
      "file": f"{AIRFLOW_HOME}/{parquet_file}"
    }
  )
  
#   bigquery_build_raw_table_task = BigQueryCreateExternalTableOperator(
#     dag=dag,
#     task_id="bigquery_build_raw_table_task",
#     table_resource={
#         "tableReference": {
#             "projectId": project_id,
#             "datasetId": bq_dataset_id,
#             "tableId": bq_table_id,
#         },
#         "externalDataConfiguration": {
#             "sourceFormat": "PARQUET",
#             "sourceUris": [f"gs://{bucket_name}/{gcs_object_path}"],
#             "autodetect": True
#         }
#     }
# )
  
  download_data_task >> parquetize_data_task >> local_to_gcs_task 

# NFL Ingest DAG
nfl_ingest_dag = DAG(
  dag_id="nfl_ingest_dag",
  default_args=default_args,
  schedule_interval = "@yearly",
  start_date=datetime(1975, 1, 1),
  # catchup will kick off a dag run for any data interval that has not been run since the last data interval
  catchup=True,
  # try to limit active concurrent runs or machine could freak out
  max_active_runs=3,
  tags=['nfl_ingest_dag']
)

# NFL Variables
# nfl_dataset_url = f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/28/schedule?season={year}"
nfl_source_file = f"processed_nfl_{year}.csv"
nfl_parquet_file = nfl_source_file.replace('.csv', '.parquet')
BUCKET_NFL = os.environ.get("GCP_GCS_BUCKET_NFL")
NFL_BIGQUERY_DATASET = os.environ.get("NFL_BIGQUERY_DATASET")
NFL_BIGQUERY_TABLEID = os.environ.get("NFL_BIGQUERY_TABLEID")
nfl_gcs_object_path = f"raw/schedule/v2/{nfl_parquet_file}"

# NFL Tasks
download_parquetize_upload_gcs_tasks(
    dag=nfl_ingest_dag,
    source_file=nfl_source_file,
    parquet_file=nfl_parquet_file,
    bucket_name=BUCKET_NFL,
    gcs_object_path=nfl_gcs_object_path,
    project_id=PROJECT_ID,
    bq_dataset_id=NFL_BIGQUERY_DATASET,
    bq_table_id=NFL_BIGQUERY_TABLEID
)

# Elections Ingest DAG
elections_ingest_dag = DAG(
  dag_id="elections_ingest_dag",
  default_args=default_args,
  schedule_interval = "@once",
  start_date=datetime(2024, 3, 6),
  catchup=False,
  # try to limit active concurrent runs or machine could freak out
  max_active_runs=3,
  tags=['elections_ingest_dag']
) 

# Elections Variables
elections_source_file = 'processed_elections.csv'
elections_parquet_file = elections_source_file.replace('.csv', '.parquet')
BUCKET_ELECTIONS = os.environ.get("GCP_GCS_BUCKET_ELECTIONS")
ELECTIONS_BIGQUERY_DATASET = os.environ.get("ELECTIONS_BIGQUERY_DATASET")
ELECTIONS_BIGQUERY_TABLEID = os.environ.get("ELECTIONS_BIGQUERY_TABLEID")
elections_gcs_object_path = f"raw/schedule/{elections_parquet_file}"

download_parquetize_upload_gcs_tasks(
    dag=elections_ingest_dag,
    source_file=elections_source_file,
    parquet_file=elections_parquet_file,
    bucket_name=BUCKET_ELECTIONS,
    gcs_object_path=elections_gcs_object_path,
    project_id=PROJECT_ID,
    bq_dataset_id=ELECTIONS_BIGQUERY_DATASET,
    bq_table_id=ELECTIONS_BIGQUERY_TABLEID
)

# Transform DAG
nfl_elec_transform_dag = DAG(
  dag_id="nfl_elec_transform_dag",
  default_args=default_args,
  schedule_interval = "@once",
  start_date=datetime(2024, 3, 6),
  catchup=False,
  # try to limit active concurrent runs or machine could freak out
  max_active_runs=3,
  tags=['nfl_elec_transform_dag']
)

nfl_elec_transform_task = SparkSubmitOperator(
    dag=nfl_elec_transform_dag,
    task_id="nfl_elec_transform_task",
    application=f"{AIRFLOW_HOME}/jobs/transform/nfl-elec-transform-v2.py",
    conn_id="spark-conn",
    jars=f"{AIRFLOW_HOME}/shared-jars/gcs-connector-hadoop3-2.2.20.jar"
)

# Load DAG
nfl_elec_upload_dag = DAG(
  dag_id="nfl_elec_upload_dag",
  default_args=default_args,
  schedule_interval = "@once",
  start_date=datetime(2024, 3, 6),
  catchup=False,
  # try to limit active concurrent runs or machine could freak out
  max_active_runs=3,
  tags=['nfl_elec_upload_dag']
)
results_object_path = "results/nfl_elec_results.parquet"
RESULTS_BUCKET = os.environ.get("GCP_GCS_BUCKET_RESULTS")
RESULTS_BIGQUERY_DATASET = os.environ.get("RESULTS_BIGQUERY_DATASET")
RESULTS_BIGQUERY_TABLEID = os.environ.get("RESULTS_BIGQUERY_TABLEID")


transform_results_upload_gcs_task = PythonOperator(
  dag=nfl_elec_upload_dag,
  task_id="transform_results_upload_gcs_task",
  python_callable=upload_to_gcs,
  op_kwargs={
    "bucket": "redskins-rule-results",
    "object_name": results_object_path,
    "file": f"{AIRFLOW_HOME}/{results_object_path}"
  }

)

bigquery_build_prod_table_task = BigQueryCreateExternalTableOperator(
  dag=nfl_elec_upload_dag,
  task_id="bigquery_build_prod_table_task",
  table_resource={
      "tableReference": {
          "projectId": PROJECT_ID,
          "datasetId": RESULTS_BIGQUERY_DATASET,
          "tableId": RESULTS_BIGQUERY_TABLEID,
      },
      "externalDataConfiguration": {
          "sourceFormat": "PARQUET",
          "sourceUris": [f"gs://{RESULTS_BUCKET}/{results_object_path}"],
          "autodetect": True
      }
  }
)

transform_results_upload_gcs_task >> bigquery_build_prod_table_task