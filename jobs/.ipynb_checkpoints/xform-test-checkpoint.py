from pyspark.sql import SparkSession

# ENV Variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NFL = os.environ.get("GCP_GCS_BUCKET_NFL")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

raw_url_nfl = f"{PROJECT_ID}-{BUCKET_NFL}/raw/schedule/*"

def get_raw_data(src_file):
    # create the spark session
    spark = SparkSession.builder.appName("GetRawDataApp").getOrCreate()
    # read parquet file from gcs
    df = spark.read.parquet(src_file)
# xform tasks:
    # drop un needed columns
    # create schema
    # map needed columns to new df
    # create bq table
    # write new df to bq table
      # partitioned by year ideally 
    # stop spark sessions