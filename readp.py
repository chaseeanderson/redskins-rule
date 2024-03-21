import pandas as pd 

df = pd.read_parquet('/Users/ceanders/code/data-eng/projects/redskins-rule/results/nfl_elec_results.parquet')
df.to_csv('here.csv')