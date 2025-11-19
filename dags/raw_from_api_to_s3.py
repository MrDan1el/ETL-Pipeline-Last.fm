from datetime import datetime, timedelta
import json
import requests
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


COUNTRIES = [
    'Russian Federation',
    'United States',
    'Kazakhstan'
]


def get_data_from_api(params):
    url = 'https://ws.audioscrobbler.com/2.0/'
    headers = {'user-agent': 'username'}
    payload = {
        'api_key': Variable.get("api_key"),
        'format': 'json'
    }
    payload.update(params) 
    response = requests.get(url, headers=headers, params=payload)
    return response.json()


def load_data_to_s3(**context):
    date = context["data_interval_end"].format("YYYY-MM-DD")
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    
    for country in COUNTRIES:
        logging.info(f">> Отправка запроса в API для {country} за {date}...")   
        params = {
            'method': 'geo.getTopTracks',
            'country': country,
            'limit': 100
        }
        data = get_data_from_api(params)
        logging.info(f">> Получены данные для {country} за {date}.")   
        
        key = f"top_100/raw/{date}/{country}_{date}.json"
        logging.info(f">> Загрузка данных в S3 для {country} за {date}...")   
        s3_hook.load_string(
            string_data=json.dumps(data, indent=4),
            key=key,
            bucket_name='bucket',
            replace=True
        )
        logging.info(f">> Загружены данные в S3 для {country} за {date}.") 


default_args = {
    'owner': 'username',
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    default_args=default_args,
    dag_id='raw_from_api_to_s3',
    description='Extract raw data from API to S3',
    tags=['raw', 's3', 'data lake', 'api'],
    start_date = days_ago(1),
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id='start'
    )

    load_data_to_s3 = PythonOperator(
        task_id='load_data_to_s3',
        python_callable=load_data_to_s3
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> load_data_to_s3 >> end