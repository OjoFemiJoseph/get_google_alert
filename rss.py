from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import dag, task
from bs4 import BeautifulSoup as bs


@dag(schedule_interval=None, start_date=datetime(2023, 5, 8), catchup=False, tags=['Data Engineering'])
def rss_pipeline():
    """
    """

    @task()
    def get_rss():
        url = "https://www.google.com/alerts/feeds/17166252119316840001/5756751131366515567"
        resp = requests.get(url)
        if resp.status_code == 200:
            resp = resp.text
            return resp
        
    @task()
    def save_to_local(data):
        df = pd.DataFrame(data)
        print(df.head())
        #you can export to where ever
        
    @task()
    def parse_xml(resp):
        soup = bs(resp, 'lxml')
        all_entries = soup.find_all('entry')
        data = []
        for entry in all_entries:
            id = entry.id.text
            title = entry.title.text
            link = entry.link['href']
            published = entry.published.text
            updated = entry.updated.text
            content = entry.content.text
            
            entry_dict = {
            "ID": id,
            "Title": title,
            "Link": link,
            "Published": published,
            "Updated": updated,
            "Content": content
            }
            data.append(entry_dict)

        return data
   
    xml_data = get_rss()
    parsed_data = parse_xml(xml_data)
    save_to_local(parsed_data)
    

rss = rss_pipeline()