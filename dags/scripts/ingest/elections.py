import os
import requests as rqst
from bs4 import BeautifulSoup as bs
import re
import pandas as pd

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
elections_source_file = 'processed_elections.csv'

class SoupScraper:
    def __init__(self, html):
        self.html = html
        self.table_head = []
        self.table_body = []
        
    def scrape(self, html):
      elections_html = rqst.get(html)
      soup = bs(elections_html.content, "html.parser")

      # Remove superscripts from html doc
      sup_tags = soup.find_all('sup')
      for tag in sup_tags:
          tag.decompose()

      # Get the headers
      table_head = soup.thead
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

elections_data = SoupScraper("https://www.britannica.com/topic/United-States-Presidential-Election-Results-1788863")
elections_data.scrape(elections_data.html)

# Formatting helpers
def is_year(input_string):
    pattern = r'^\d{4}$'
    if re.match(pattern, input_string):
        return True
    else:
        return False

def insert_years(table):
    for idx, row in enumerate(table):
        if idx > 0:
            prev_row = idx - 1
        else: 
            prev_row = 0
        if is_year(row[0]) == False:
            row.insert(0, table[prev_row][0])

# Format the election data
insert_years(elections_data.table_body)

df_pandas = pd.DataFrame(elections_data.table_body, columns=elections_data.table_head)
df_pandas.to_csv(f"{AIRFLOW_HOME}/{elections_source_file}")

