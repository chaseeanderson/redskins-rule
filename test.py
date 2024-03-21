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