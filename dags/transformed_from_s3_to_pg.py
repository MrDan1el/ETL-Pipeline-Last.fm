from datetime import datetime, timedelta
import logging
import json
import csv
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def extract_keys_from_S3(**context):
    date = context["data_interval_end"].format("YYYY-MM-DD")
    s3 = S3Hook(aws_conn_id='aws_conn')

    logging.info(">> Получение ключей из S3...")
    keys = s3.list_keys(
        bucket_name='bucket', 
        prefix=f"top_100/raw/{date}/"
    ) or []
    logging.info(f">> Получены ключи из S3 за {date}.")

    context['ti'].xcom_push(key='s3_keys', value=keys)


def get_transformed_data(data, date, country):
    transformed_data = []
    tracks_list = data['tracks']['track']
    for track in tracks_list:
        track_info = (
            track['name'],                  #song_name
            track['artist']['name'],        #artist_name
            int(track['duration']),         #duration_sec
            int(track['listeners']),        #listeners_count
            int(track['@attr']['rank']),    #song_rank
            date,                           #source_date
            country                         #country
        )
        transformed_data.append(track_info)
    return transformed_data


def transform_data_csv(**context):
    date = context["data_interval_end"].format("YYYY-MM-DD")
    keys = context['ti'].xcom_pull(task_ids='extract_keys_from_S3', key='s3_keys')

    csv_file_path = os.getcwd() + '/temp_data_file.csv'
    s3_hook = S3Hook(aws_conn_id='aws_conn')

    logging.info(">> Создание временного CSV файла для записи данных...")
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(('song_name', 'artist_name', 'duration_sec', 'listeners_count', 'song_rank', 'source_date', 'country'))

        logging.info(">> Получение данных по ключам из S3...")
        for key in keys:
            obj = s3_hook.read_key(key=key, bucket_name='bucket')
            raw_data = json.loads(obj)
            country = key.split('/')[-1].split('_')[0]
            logging.info(f">> Трансформация и запись данных в CSV файл для {country} за {date}...")
            transformed_data = get_transformed_data(raw_data, date, country)
            writer.writerows(transformed_data)
    logging.info(f">> Записан CSV файл с данными за {date}.")

    context['ti'].xcom_push(key='csv_file_path', value=csv_file_path)


def load_data_to_pg(**context):
    date = context["data_interval_end"].format("YYYY-MM-DD")
    csv_file_path = context['ti'].xcom_pull(task_ids='transform_data_csv', key='csv_file_path')

    pg_hook = PostgresHook(postgres_conn_id='pg_conn')
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    
    logging.info(">> Копирование данных из CSV файла в таблицу temp_daily_data...")
    with open(csv_file_path, 'r', newline='') as file:
        cur.execute("TRUNCATE TABLE ods.temp_daily_data")
        cur.copy_expert(
            """
            COPY ods.temp_daily_data (song_name, artist_name, duration_sec, listeners_count, song_rank, source_date, country)
            FROM STDIN 
            WITH CSV HEADER DELIMITER AS ';'
            """,
            file
        )
    conn.commit()
    logging.info(f"Загружены данные в temp_daily_data за {date}.")

    os.remove(csv_file_path)
    logging.info(">> Временный CSV файл удален.")


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

    extract_keys_from_S3 = PythonOperator(
        task_id='extract_keys_from_S3',
        python_callable=extract_keys_from_S3
    )

    transform_data_csv = PythonOperator(
        task_id='transform_data_csv',
        python_callable=transform_data_csv
    )

    load_data_to_pg = PythonOperator(
        task_id='load_data_to_pg',
        python_callable=load_data_to_pg
    )

    insert_into_ods_table = SQLExecuteQueryOperator(
        task_id="insert_into_ods_table",
        conn_id="pg_conn",
        sql = '''
            INSERT INTO ods.daily_data (song_name, artist_name, duration_sec, listeners_count, song_rank, source_date, country)
            SELECT *
            FROM ods.temp_daily_data
            ON CONFLICT (song_rank, source_date, country) DO NOTHING
        '''
    )

    end = EmptyOperator(
        task_id="end"
    )    

    start >> sensor_on_raw_layer >> extract_keys_from_S3 >> transform_data_csv >> load_data_to_pg >> insert_into_ods_table >> end
