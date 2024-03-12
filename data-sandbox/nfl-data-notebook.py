import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from google.cloud import storage
from pyspark.sql.functions import col, translate

credentials_location = '/Users/ceanders/.google/credentials/projects/redskins-rule/redskins-rule-docker-airflow-credentials.json'

conf = SparkConf() \
    .setMaster('local[*]') \
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

# Function to read schema from a Parquet file and explode fields
def get_raw_data_and_explode(file_path):
    df = spark.read.parquet(file_path)
    count = df.count()
    print(f"exploding data complete...here's the row count {count}")
    exploded_df = df.withColumn('exp_events', F.explode('events'))
    exploded_df = exploded_df.withColumn('exp_competitions', F.explode('exp_events.competitions'))
    exploded_df = exploded_df.withColumn('exp_competitors', F.explode('exp_competitions.competitors'))
    return exploded_df

# Define empty df to save to
empty_RDD = spark.sparkContext.emptyRDD()
columns = StructType([
    StructField("date", TimestampNTZType(), True),
    StructField("id", StringType(), True),
    StructField("value", DoubleType(), True)
])
processed_df = spark.createDataFrame(data = empty_RDD, schema = columns)

# Get files from GCS and process
def list_blobs_with_prefix(bucket_name, prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return ["gs://" + bucket_name + "/" + blob.name for blob in blobs]

bucket_name = 'redskins-rule-nfl-game-data'
prefix = 'raw/schedule/'
file_paths = list_blobs_with_prefix(bucket_name, prefix)

for file_path in file_paths:
    print(f"grabbing file...{file_path}")
    exploded_df = get_raw_data_and_explode(file_path)
    
    # create a temp table
    exploded_df.createOrReplaceTempView('temp')

    # transform the file and save to a df
    xform_df = spark.sql("""
    SELECT 
        exp_events.date,
        exp_competitors.id,
        exp_competitors.score.value
    FROM
        temp
    GROUP BY 
        1,2,3
    """)
    print("xform df row count")
    count = xform_df.count()
    print(f"transforming data complete...here's the row count {count}") 
    
    # union to the processed df
    processed_df = processed_df.unionByName(xform_df)
    proc_count = processed_df.count()
    print(f"processsed count...{proc_count}")

processed_df.select('*').sort('date', ascending=False).show()

spark.stop()

def predict_pres(df):
    for elem in df: 
        if elem.redskins_result == 'WIN':
            return elem.incumbent_pres_party
        else: 
            return elem.challenger_pres_party

def predict_pres_flipped(df):
    for elem in df:
        if elem.redskins_result == 'LOSE':
            return elem.incumbent_pres_party
        else: 
            return elem.challenger_pres_party
        
def rule_check(df):
    for elem in df:
        if elem.pop_winning_party != elem.pres_winning_party:
            return predict_pres_flipped(df)
        else:
            return predict_pres(df)
        
# V2
def predict_pres(df_elem):
    if df_elem.redskins_result == 'WIN':
        print('norm - win')
        print(df_elem.elec_date)
        print(df_elem.incumbent_pres_party)
        return df_elem.incumbent_pres_party
    else: 
        print('norm - lose')
        print(df_elem.elec_date)
        print(df_elem.challenger_pres_party)
        return df_elem.challenger_pres_party

def predict_pres_flipped(df_elem):
    if df_elem.redskins_result == 'LOSE':
        print('flipped! lose')
        print(df_elem.elec_date)
        print(df_elem.incumbent_pres_party)
        return df_elem.incumbent_pres_party
    else: 
        print('flipped! win')
        print(df_elem.elec_date)
        print(df_elem.challenger_pres_party)
        return df_elem.challenger_pres_party
        
def rule_check(df_elem):
    if df_elem.pop_winning_party != df_elem.pres_winning_party:
        return predict_pres_flipped(df_elem)
    else:
        return predict_pres(df_elem)
    
    def add_new_column(df, column_name, value):
        df = df.withColumn(column_name, F.lit(value))
        return df

    # Usage
    processed_df = add_new_column(processed_df, "new_column", "new_value")
    
    
def row_rule_check(elem, toggle=determine_rule(test_df_elem[0])):
    # toggle = determine_rule(elem)
    # for elem in df_elems:
    print(f"toggle: {toggle}")
    # check toggle for which function to run 
    if toggle == 1:
        return predict_pres(elem)
    elif toggle == -1:
        return predict_pres_flipped(elem)
    else: 
        print('somethings up')
    # determine toggle for next iteration
    toggle = determine_rule(elem)
    return elem
        
def check_rule(pres_winning_party, prediction):
    # if df.pres_winning_party == df.prediction:
    #     return TRUE
    # else:
    #     return FALSE
    return True if pres_winning_party == prediction else False