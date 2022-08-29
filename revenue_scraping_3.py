

#pip install requests_html

import os
import sys
from datetime import date
import requests
import pandas as pd
from requests_html import HTML


BASE_DIR = "F://imdb_data//"

current_year = date.today().year
year=str(current_year)
start_year = int(year)
def url_to_txt(url, filename="world.html", save=False):
    r = requests.get(url)
    if r.status_code == 200:
        html_text = r.text
        if save:
            with open(f"world-{year}.html", 'w','utf-8') as f:
                f.write(html_text)
        return html_text
    print("world.html created")
    return None



def parse_and_extract(url, name='2022'):
    
    html_text = url_to_txt(url)
    if html_text == None:
        return False
    r_html = HTML(html=html_text)
    
    table_class = ".imdb-scroll-table"
    # table_class = "#table"
    r_table = r_html.find(table_class)

    # print(r_table)
    table_data = []
    table_data_dicts = []
    header_names = []
    
    if len(r_table) == 0:
        return False
    parsed_table = r_table[0]
    rows = parsed_table.find("tr")
    header_row = rows[0]
    header_cols = header_row.find('th')
    header_names = [x.text for x in header_cols]
    
    for row in rows[1:]:
        # print(row.text)
      cols = row.find("td")
      row_data = []
      row_dict_data = {}
      
      for i, col in enumerate(cols):
            # print(i, col.text, '\n\n')
            #header_name = header_names[i]
            # row_dict_data[header_name] = col.text
            row_data.append(col.text)
      table_data_dicts.append(row_dict_data)
      
      table_data.append(row_data)
        
    print("df created")
    df = pd.DataFrame(table_data, columns=header_names)
    # df = pd.DataFrame(table_data_dicts)
    path = os.path.join(BASE_DIR, '//data')
    print(path)
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join('F://imdb_data//', f'//{name}.csv')
    df.to_csv(filepath, index=False)
    print("file created")
    return True

def run(start_year=None, years_ago=0):
    if start_year == 2022:
       
        
        for i in range(0, 1):
            print(i)
            url = f"https://www.boxofficemojo.com/year/world/{start_year}/"
            print("parse")
            finished = parse_and_extract(url, name=start_year)
        if finished:
            print(f"Finished {start_year}")
        else:
            print(f"{start_year} not finished")
        start_year -= 1



if __name__ == "__main__":
    try:
        start = 2022
    except:
        start = None
    try:
        count = 1
    except:
        count = 0
    print("run")
    run(start_year=start, years_ago=count)

from google.colab import files

files.download('/2022.csv')