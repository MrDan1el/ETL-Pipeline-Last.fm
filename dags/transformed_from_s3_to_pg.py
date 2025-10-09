from datetime import datetime, timedelta
import logging
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def discover_files_S3(**context):
    
    date = context["data_interval_end"].format("YYYY-MM-DD")

    s3 = S3Hook(aws_conn_id='aws_conn')
    keys = s3.list_keys(
        bucket_name='bucket', 
        prefix=f"top_100/raw/{date}/"
    ) or []
    
    logging.info(f"Получены ключи за {date} из S3") 
    context['ti'].xcom_push(key='s3_keys', value=keys)


def transform_data(data, date, country):
    
    transformed_data = []
    tracks_list = data['tracks']['track']
    for track in tracks_list:
        track_info = {
            'song_name': track['name'],
            'duration_sec': int(track['duration']),
            'listeners_count': int(track['listeners']),
            'artist_name': track['artist']['name'],
            'song_rank': int(track['@attr']['rank']),
            'source_date': date,
            'country': country
        }
        transformed_data.append(track_info)    
    return transformed_data


def load_data_to_pg(**context):

    date = context["data_interval_end"].format("YYYY-MM-DD")
    keys = context['ti'].xcom_pull(task_ids='discover_files_S3', key='s3_keys')

    s3_hook = S3Hook(aws_conn_id='aws_conn')
    pg_hook = PostgresHook(postgres_conn_id='pg_conn')
    insert_query = """
        INSERT INTO ods.daily_data (song_name, artist_name, duration_sec, listeners_count, song_rank, source_date, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (song_rank, source_date, country) DO NOTHING
        """
    
    for key in keys:
        obj = s3_hook.read_key(
            key=key, 
            bucket_name='bucket'
        )
        country = key.split('/')[-1].split('_')[0]
        data = transform_data(json.loads(obj), date, country)

        for track in data:
            pg_hook.run(
                insert_query, 
                parameters = (
                    track['song_name'], 
                    track['artist_name'], 
                    track['duration_sec'], 
                    track['listeners_count'], 
                    track['song_rank'], 
                    track['source_date'], 
                    track['country']
                )
            )
        logging.info(f"Данные для {country} за {date} загружены в Postgres ods") 


default_args = {
    'owner': 'username',
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    default_args=default_args,
    dag_id='transformed_from_s3_to_pg',
    description='Extract, transform and load data from S3 to Postgres ods schema',
    tags=['s3', 'postgres', 'api', 'ods', 'etl'],
    start_date = days_ago(1),
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id="start"
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=60  # частота проверки
    )

    discover_files_S3 = PythonOperator(
        task_id='discover_files_S3',
        python_callable=discover_files_S3
    )

    load_data_to_pg = PythonOperator(
        task_id='load_data_to_pg',
        python_callable=load_data_to_pg
    )

    end = EmptyOperator(
        task_id="end"
    )    

    start >> sensor_on_raw_layer >> discover_files_S3 >> load_data_to_pg >> end
