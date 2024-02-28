import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

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

# Process files
file_paths = [
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2000.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2001.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2002.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2003.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2004.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2005.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2006.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2007.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2008.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2009.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2010.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2011.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2012.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2013.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2014.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2015.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2016.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2017.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2018.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2019.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2020.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2021.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2022.parquet',
    'gs://redskins-rule-nfl-game-data/raw/schedule/nfl_season_2023.parquet',
    
]

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