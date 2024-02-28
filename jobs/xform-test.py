import os
import pyspark
import argparse
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# UNCOMMENT BELOW FOR LOCAL RUN #
credentials_location = '/Users/ceanders/.google/credentials/projects/redskins-rule/redskins-rule-docker-airflow-credentials.json'

conf = SparkConf() \
    .setAppName('test') \
    .set("spark.jars", "/usr/local/Cellar/apache-spark/3.5.0/libexec/jars/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .appName('test') \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# ENV Variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NFL = os.environ.get("GCP_GCS_BUCKET_NFL")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

raw_url_nfl = f"{PROJECT_ID}-{BUCKET_NFL}/raw/schedule/*"

def get_raw_data(src_file):
    # create the spark session (done above)
    # read parquet file from gcs
    df = spark.read.parquet(src_file)
    df.show(5)
# xform tasks:
    # drop un needed columns
    # create schema
    # map needed columns to new df
    # create bq table
    # write new df to bq table
      # partitioned by year ideally 
    # stop spark sessions

get_raw_data(raw_url_nfl)