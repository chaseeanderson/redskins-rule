import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

def test_function():
  print("HELLO WORLD")

def test_function_dag(dag):
  with dag:
    test_print_task = PythonOperator(
      task_id="test_print_task",
      python_callable=test_function
    )

    test_print_task

default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

test_dag = DAG(
  dag_id="test_dag",
  default_args=default_args,
  # schedule_interval = "0 6 2 * *",
    # one time run to create new tables with all files
    schedule_interval = "@once",
    start_date=datetime(2019, 1, 1),
    # catchup will kick off a dag run for any data interval that has not been run since the last data interval
    catchup=True,
    # try to limit active concurrent runs or machine could freak out
    max_active_runs=3,
    tags=['dtc-de']
)

test_function_dag(
  dag=test_dag
)