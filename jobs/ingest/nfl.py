import os
import requests as rqst
from bs4 import BeautifulSoup as bs
import pandas as pd
from itertools import chain
from datetime import datetime

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

class SoupScraper:
    def __init__(self, html):
        self.html = html
        self.table_head = []
        self.table_body = []
        
    def scrape(self, html, headers):
      nfl_html = rqst.get(html, headers=headers)
      soup = bs(nfl_html.content, "html.parser")

      schedule = soup.find_all('thead')
      schedule_details = soup.find_all('tbody')
       
      # Get the table headers for each week of the schedule  
      for week in schedule:
          table_head = week
          row_headers = []
          for header in table_head.find_all('tr'):
              for header_value in header.find_all('th'):
                  row_headers.append(header_value.get_text(strip=True))    
          self.table_head.append(row_headers)
      # Get the table values
      for idx, week_details in enumerate(schedule_details):
          table_body = week_details
          table_values = []
          for row in table_body.find_all('tr'):
              td_tags = row.find_all('td')
              for value in td_tags:
                  td_val = [value.get_text(strip=True) for value in td_tags]
                  # td_val = value.get_text(strip=True)
                  td_val.insert(0, self.table_head[idx][0])
              table_values.append(td_val)
          self.table_body.append(table_values)

def ingest_nfl(**context):
    ds = context['ds']
    year = datetime.strptime(ds, "%Y-%m-%d").year
    if year < 2020:
        url = f"https://www.footballdb.com/teams/nfl/washington-redskins/results/{year}"
    elif year >= 2020 and year < 2022:
        url = f"https://www.footballdb.com/teams/nfl/washington-football-team/results/{year}"
    else:
        url = f"https://www.footballdb.com/teams/nfl/washington-commanders/results/{year}"
    print(f"URL HERE...{url}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }

    nfl_data = SoupScraper(url)
    nfl_data.scrape(nfl_data.html, headers)

    # unnest data for pd df
    unnested_data = list(chain.from_iterable(nfl_data.table_body))

    # add processing year
    for row in unnested_data:
        row.insert(0, year)

    # select elements for pd df
    trimmed_unnested_data = []
    for row in unnested_data:
        trimmed_row = []
        trimmed_row.append(row[0])
        trimmed_row.append(row[1])
        trimmed_row.append(row[2])
        trimmed_row.append(row[len(row) - 1])
        trimmed_unnested_data.append(trimmed_row)

    columns = ['year', 'date', 'team', 'final']
    df_pandas = pd.DataFrame(trimmed_unnested_data, columns=columns)
    
    print(df_pandas)

    nfl_source_file = f"processed_nfl_{year}.csv"
    df_pandas.to_csv(f"{AIRFLOW_HOME}/{nfl_source_file}")