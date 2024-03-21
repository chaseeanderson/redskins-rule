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

      # multiple theads for each week of the schedule
      schedule = soup.find_all('thead')
      data = []
      for week in schedule:
        table_head = week
        row_headers = []
        for header in table_head.find_all('tr'):
            for header_value in header.find_all('th'):
                row_headers.append(header_value.get_text(strip=True))    
        self.table_head = row_headers

        # Get the table values
        table_body = soup.tbody
        table_values = []
        for row in table_body.find_all('tr'):
            td_tags = row.find_all('td')
            for value in td_tags:
                td_val = [value.get_text(strip=True) for value in td_tags]
            table_values.append(td_val)
        self.table_body = table_values
        # insert the game date from the thead into the tbody
        for row in self.table_body:
            row.insert(0, self.table_head[0])
        data.append(self.table_body)
      self.table_body = data

def ingest_nfl(**context):
    ds = context['ds']
    year = datetime.strptime(ds, "%Y-%m-%d").year
    if year < 2020:
        url = f"https://www.footballdb.com/teams/nfl/washington-redskins/results/{year}"
    else:
        url = f"https://www.footballdb.com/teams/nfl/washington-football-team/results/{year}"
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

    columns = ['year', 'date', 'team', 'q1', 'q2', 'q3', 'q4', 'final']
    df_pandas = pd.DataFrame(unnested_data, columns=columns)
    
    print(df_pandas)

    nfl_source_file = f"processed_nfl_{year}.csv"
    df_pandas.to_csv(f"{AIRFLOW_HOME}/{nfl_source_file}")