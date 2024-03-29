import pyspark
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from google.cloud import storage
from datetime import datetime, timedelta

GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME')

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('redskins-rule-spark') \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.project.id", "project-redskins-rule")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# NFL #

def get_raw_nfl_data(file_path):
    df = spark.read.parquet(file_path)
    return df

# Get files from GCS and process
def list_blobs_with_prefix(bucket_name, prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    return ["gs://" + bucket_name + "/" + blob.name for blob in blobs]

nfl_bucket_name = 'redskins-rule-nfl-game-data'
nfl_prefix = 'raw/schedule/v2/'
nfl_file_paths = list_blobs_with_prefix(nfl_bucket_name, nfl_prefix)

# Define empty df to save to
empty_RDD = spark.sparkContext.emptyRDD()
nfl_columns = StructType([
    StructField("", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("team", StringType(), True),
    StructField("final", StringType(), True)
])
nfl_df = spark.createDataFrame(data = empty_RDD, schema = nfl_columns)

for file_path in nfl_file_paths:
    print(f"grabbing file...{file_path}")
    df = get_raw_nfl_data(file_path)
    nfl_df = nfl_df.unionByName(df)

# field formatting
date_regex = '((0?[1-9]|1[0-2])/([12][0-9]|3[01]|0?[1-9]))'
nfl_df = nfl_df.withColumn('day_month', F.regexp_extract(nfl_df.date, date_regex, 0))
nfl_df = nfl_df.withColumn('game_date', F.concat(nfl_df.day_month, F.lit('/'), nfl_df.year))

nfl_df.createOrReplaceTempView('temp')
nfl_filter_reg_season_sql = """
  SELECT *
  FROM temp
  WHERE LEN(date) < 10
"""
nfl_df = spark.sql(nfl_filter_reg_season_sql)

nfl_df = nfl_df.withColumn('game_date', F.to_date(nfl_df.game_date, 'M/d/yyyy'))
nfl_df = nfl_df.withColumn('team', F.split(nfl_df.team, '\('))
nfl_df = nfl_df.withColumn('team', F.element_at(nfl_df.team, 1))
nfl_df = nfl_df.withColumn('final', nfl_df.final.cast(IntegerType()))
nfl_df = nfl_df.withColumnRenamed('', 'row_num')
nfl_df = nfl_df.dropDuplicates()
nfl_df.createOrReplaceTempView('nfl_df')

process_nfl_sql = """
SELECT
  *,
  CASE
    WHEN team = winning_team THEN 'WIN'
    ELSE 'LOSE'
  END as redskins_result
FROM (
  SELECT 
    *,
    MAX_BY(team, final_score) OVER(PARTITION BY game_date) as winning_team,
    MAX_BY(final_score, final_score) OVER(PARTITION BY game_date) as winning_team_score
  FROM (
    SELECT
      game_date,
      team,
      final as final_score,
      -- raw data is ordered to present the home team as the second row in each game
      RANK() OVER(PARTITION BY game_date ORDER BY row_num) as home_team_rnk
    FROM 
        nfl_df    
  )
)
WHERE
  team = 'Washington' AND home_team_rnk = 2
"""
nfl_df = spark.sql(process_nfl_sql)
nfl_df.createOrReplaceTempView('nfl_df')

# ELECTIONS #

def get_raw_elections_data(file_path):
    df = spark.read.parquet(file_path)
    return df

elec_bucket_name = 'redskins-rule-presidential-election-data'
elec_prefix = 'raw/'
elec_file_paths = list_blobs_with_prefix(elec_bucket_name, elec_prefix)

elec_columns = StructType([
    StructField("row_num", LongType(), True),
    StructField("year", StringType(), True),
    StructField("candidate", StringType(), True),
    StructField("political_party", StringType(), True),
    StructField("electoral_votes", LongType(), True),
    StructField("popular_votes", StringType(), True),
    StructField("popular_percentage", StringType(), True),
])
elec_df = spark.createDataFrame(data = empty_RDD, schema = elec_columns)

for file_path in elec_file_paths:
    print(f"grabbing file...{file_path}")
    df = get_raw_elections_data(file_path)
    elec_df = elec_df.unionAll(df)

# field formatting
elec_df = elec_df.withColumn('year', F.to_date(elec_df.year, 'yyyy'))
elec_df = elec_df.withColumn('popular_votes', F.translate(elec_df.popular_votes, ",", "").cast(LongType()))

# filter for years > 1972 for incumbent status
query = "year >= DATE '1972-01-01'"
elec_df = elec_df.where(query)

# Add election dates to elections df:
# Get date of election by election year
def find_election_day(year):
    # Find the first day of November
    date = datetime(year, 11, 1)
    # If this day is not Monday, find the next Monday
    while date.weekday() != 0:
        date += timedelta(days=1)
    # The election day is the next Tuesday
    date += timedelta(days=1)
    return date.strftime("%Y-%m-%d")

# Find the election days every 4 years starting in the year 1976
elec_dates_values = [find_election_day(year) for year in range(1976, datetime.now().year + 1, 4)]

# convert to pandas df to load to spark
pd_df = pd.DataFrame(elec_dates_values, columns=['elec_date'])

# build into spark df to to elec_df
elec_dates_columns = StructType([
    StructField("elec_date", StringType(), True),
])
elec_dates_df = spark.createDataFrame(data = pd_df, schema = elec_dates_columns)
elec_dates_df = elec_dates_df.withColumn('elec_date', F.to_date(elec_dates_df.elec_date, 'yyyy-MM-dd'))

elec_df.createOrReplaceTempView('elec_df')
elec_dates_df.createOrReplaceTempView('elec_dates_df')

dates_join_query = """
    SELECT e.*, ed.elec_date
    FROM elec_df e 
    LEFT JOIN (SELECT DISTINCT elec_date FROM elec_dates_df) ed ON DATE_TRUNC('year', e.year) = DATE_TRUNC('year', ed.elec_date)
"""
elec_df = spark.sql(dates_join_query)

# calculate winning party metrics
elec_df.createOrReplaceTempView('elec_df')
elec_win_metrics_query = """
SELECT
  *,
  CASE WHEN (pres_winning_party = pop_incumbent_party) THEN 'WIN'
  ELSE 'LOSE'
  END as pop_incumbent_elec_result
FROM (
  SELECT
    *,
    LAG(pres_winning_party, 1) OVER (ORDER BY elec_date) as incumbent_pres_party,
    LAG(pop_winning_party, 1) OVER (ORDER BY elec_date) as pop_incumbent_party
  FROM (
    SELECT
      elec_date,
      MAX_BY(political_party, electoral_votes) as pres_winning_party,
      MAX_BY(candidate, electoral_votes) as pres_winning_candidate,
      MAX_BY(electoral_votes, electoral_votes) as count_electoral_votes,
      MAX_BY(popular_votes, popular_votes) as count_popular_votes,
      pop_winning_candidate,
      pop_winning_party,
      electoral_rank_desc,
      popular_rank_desc,
      challenger_pres_party
    FROM (
      SELECT 
        foo.*,
        bar.challenger_pres_party,
        RANK() OVER (PARTITION BY foo.elec_date ORDER BY electoral_votes DESC) as electoral_rank_desc,
        RANK() OVER (PARTITION BY foo.elec_date ORDER BY popular_votes DESC) as popular_rank_desc
      FROM (
        SELECT
          elec_date,
          candidate,
          political_party,
          electoral_votes,
          popular_votes,
          MAX_BY(candidate, popular_votes) OVER (PARTITION BY elec_date) as pop_winning_candidate,
          MAX_BY(political_party, popular_votes) OVER (PARTITION BY elec_date) as pop_winning_party
        FROM
          elec_df
      ) foo
      LEFT JOIN (
        SELECT 
          elec_date,
          political_party as challenger_pres_party
        FROM (
          SELECT 
            *,
            RANK() OVER (PARTITION BY elec_date ORDER BY electoral_votes DESC) as electoral_rank_desc
          FROM (
            SELECT
              *,
              LAG(pres_winning_party, 1) OVER (ORDER BY elec_date) as prev_winning_party
            FROM (
              SELECT
              elec_date,
              political_party,
              electoral_votes,
              MAX_BY(political_party, electoral_votes) OVER(PARTITION BY elec_date) as pres_winning_party
            FROM 
              elec_df
            )
          )
          WHERE 
            political_party <> prev_winning_party
        )
        WHERE 
          electoral_rank_desc = 1
      ) bar
      ON foo.elec_date = bar.elec_date
    )
    WHERE 
      electoral_rank_desc = 1
    GROUP BY 
      elec_date,
      electoral_rank_desc,
      popular_rank_desc,
      pop_winning_candidate,
      pop_winning_party,
      challenger_pres_party
  )
)
"""
elec_df = spark.sql(elec_win_metrics_query)
elec_df.createOrReplaceTempView('elec_df')

elec_nfl_join_query = """
SELECT *
FROM (
  SELECT 
    *,
    RANK() OVER(PARTITION BY elec_date ORDER BY date_diff ASC) diff_rank_asc
  FROM (
    SELECT *,
    DATEDIFF(day, n.game_date, e.elec_date) date_diff
    FROM
      elec_df e
    LEFT JOIN 
      nfl_df n
    ON (DATEDIFF(day, n.game_date, e.elec_date) BETWEEN 0 AND 30)
  )
)
WHERE diff_rank_asc = 1
"""
nfl_elec_df = spark.sql(elec_nfl_join_query)

# ADD PREDICTIONS #

def predict_pres(df_elem):
    if df_elem.redskins_result == 'WIN':
        return [df_elem.elec_date, df_elem.incumbent_pres_party]
    else: 
        return [df_elem.elec_date, df_elem.challenger_pres_party]

def predict_pres_flipped(df_elem):
    if df_elem.redskins_result == 'LOSE':
        return [df_elem.elec_date, df_elem.incumbent_pres_party]
    else: 
        return [df_elem.elec_date, df_elem.challenger_pres_party]
        
def determine_rule(df_elem):
    if df_elem.pop_winning_party != df_elem.pres_winning_party:
        return -1
    else:
        return 1

def get_prediction_values(df_elems):
    prediction_values = []
    rule_toggle = determine_rule(df_elems[0])
    for elem in df_elems:
        # check toggle for which function to run 
        if rule_toggle == 1:
            prediction_values.append(predict_pres(elem))
        elif rule_toggle == -1:
            prediction_values.append(predict_pres_flipped(elem))
        else: 
            print('TOGGLE ERROR')
        # determine toggle for next iteration
        rule_toggle = determine_rule(elem)
    return prediction_values

spark.udf.register('predict_pres', predict_pres)
spark.udf.register('predict_pres_flipped', predict_pres_flipped)
spark.udf.register('determine_rule', determine_rule)
spark.udf.register('get_prediction_values', get_prediction_values)

# get df elems to iterate through for prediction functions
nfl_elec_df_elems = nfl_elec_df.collect()
prediction_values = get_prediction_values(nfl_elec_df_elems)

# create df of prediction values and join to nfl_elec_df
pd_df = pd.DataFrame(prediction_values, columns=['p_elec_date', 'prediction'])
predictions_df = spark.createDataFrame(pd_df)

nfl_elec_df = nfl_elec_df.join(predictions_df, predictions_df.p_elec_date == nfl_elec_df.elec_date, 'left')

# check prediction against elec result
def check_rule(pres_winning_party, prediction):
    return pres_winning_party == prediction

spark.udf.register('check_rule', check_rule)

nfl_elec_df = nfl_elec_df.withColumn('prediction_results', check_rule(nfl_elec_df.pres_winning_party, nfl_elec_df.prediction))
nfl_elec_df = nfl_elec_df.where(nfl_elec_df.elec_date >= '1976-01-01')

# save final result
nfl_elec_df.toPandas().to_parquet(f"{AIRFLOW_HOME}/results/nfl_elec_results.parquet")

spark.stop()