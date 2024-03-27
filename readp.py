import pandas as pd 

df = pd.read_parquet('gs://redskins-rule-nfl-game-data/raw/schedule/v2/processed_nfl_1978.parquet')
df.to_csv('here.csv')