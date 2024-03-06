import re

# Elections data-secific helpers
def is_year(input_string):
    pattern = r'^\d{4}$'
    if re.match(pattern, input_string):
        return True
    else:
        return False

def insert_years(table_body):
    for idx, row in enumerate(table_body):
        if idx > 0:
            prev_row = idx - 1
        else: 
            prev_row = 0
        if is_year(row[0]) == False:
            row.insert(0, table_body[prev_row][0])
