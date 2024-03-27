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