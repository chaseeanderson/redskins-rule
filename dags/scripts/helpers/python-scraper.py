import requests as rqst
from bs4 import BeautifulSoup as bs

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