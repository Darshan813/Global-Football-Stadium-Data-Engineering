import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json
import pandas as pd


def get_wikipedia_page(url):
    try:
        response =  requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as  e:
        print(f"An error occured: {e} ")

def get_wikipedia_data(html):

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find_all("table", {"class": "wikitable sortable"})[0]
    table_rows = table.find_all("tr")
    list =[]   
    
    for i in range(1, len(table_rows)):
        data = table_rows[i].find_all("td") #will get a list of tags with td

        values = {
            "Rank": i,
            "Stadium": data[0].text,
            "Capacity": data[1].text,
            "region": data[2].text,
            "country": data[3].text,
            "city": data[4].text,
            "images": "https://" + data[5].find("img").get("src").split("//")[1] if data[5].find("img") else "No Image",
            "home_team": data[6].text
        }

        list.append(values)

    return list
        
def extract_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    data = get_wikipedia_data(html)
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    return "OK"
    
def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids ="extract_data")
    data = json.loads(data)
    for i in data:

        if len(i["Capacity"].split(' ')) > 1:
            i["Capacity"] = i["Capacity"].split(' ')[0].strip()
        else:
            i["Capacity"] = i["Capacity"].split('[')[0].strip()
        i["Stadium"] = i["Stadium"].split('♦')[0].strip()
        i["country"] = i["country"].split('♦')[0].strip()
        i["region"] = i["region"].replace("\n", "")
        state = i["city"].split(',')
        if(len(state) > 1):
            i["state"] = i["city"].split(',')[1].strip()
        else:
            i["state"] = 'No State'

        i["city"] = i["city"].split(",")[0].strip()

        if i["home_team"].strip() == "":
            i["home_team"] = "No Team"

        i["Capacity"] = i["Capacity"].replace(",", "")

    df = pd.DataFrame(data)
    df["Capacity"] = df["Capacity"].astype(int)
    json_data = df.to_json()  
    kwargs["ti"].xcom_push(key="rows", value=json_data)

def load_data(**kwargs):
    from datetime import datetime
    data = kwargs["ti"].xcom_pull(key="rows", task_ids="transform_data")
    data = json.loads(data)
    kwargs['ti'].xcom_push(key="rows", value=data)
    df = pd.DataFrame(data)
    curr_date = str(datetime.now().date())
    # df.to_csv(f"/opt/airflow/csv_files/cleaned_data_{curr_date}.csv", index=False)
    df.to_csv('abfs://footballdataengcontainer@footballdataengstorage.dfs.core.windows.net/raw_data/' + f"cleaned_data_{curr_date}.csv",
        storage_options={
                'account_key': 'x1iVC0uY0PuNgxoKO9b/H79P4K4XQcG7nKaU6CJSIiV3zJXcT77KRBI1Yq48CK0zvgJHCHhFqmOM+AStcQG3GQ=='
            }, index=False)

dag = DAG(
    dag_id= 'wikipedia_flow',                            
    default_args = {
        "owner" : "Darshan Rathod",
        "start_date": datetime(2023, 12, 30)
    },
    schedule_interval = '36 20 * * *',
    catchup = False
)

extract_data_from_wikipedia = PythonOperator(
    task_id = "extract_data",
    python_callable = extract_data,
    op_kwargs = {
        "url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
    },
    dag=dag
)

transform_wikipedia_data = PythonOperator(
    task_id = "transform_data",
    python_callable= transform_data,
    provide_context =  True,
    dag = dag
)

load_wikipedia_data = PythonOperator(
    task_id = "load_data",
    python_callable = load_data,
    dag = dag
)

extract_data_from_wikipedia >> transform_wikipedia_data >> load_wikipedia_data