from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor


DATE = datetime.now().strftime('%Y-%m-%d')


default_args = {
    'owner': 'username',
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG(
    default_args=default_args,
    dag_id='from_dds_to_dm_pg',
    description='Agregate and insert data from dds to data marts in Postgres',
    tags=['dds', 'dm', 'postgres', 'data mart'],
    start_date = days_ago(1),
    schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    
    start = EmptyOperator(
        task_id="start"
    )

    sensor_on_dds = ExternalTaskSensor(
        task_id="sensor_on_dds",
        external_dag_id="from_ods_to_dds_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,
        poke_interval=60 
    )

    insert_into_avg_song_duration_by_country = SQLExecuteQueryOperator(
        task_id="insert_into_avg_song_duration_by_country",
        conn_id="pg_conn",
        sql = '''
            INSERT INTO dm.avg_song_duration_by_country
                SELECT date, country_name, AVG(duration_sec)
                FROM dds.fact_daily_top_100
                    JOIN dds.dim_song USING(song_id)
                    JOIN dds.dim_country USING(country_id)
                WHERE date = %(date)s    
                GROUP BY date, country_name
        ''',
        parameters={"date": DATE}
    )

    insert_into_artist_appearances_by_date = SQLExecuteQueryOperator(
        task_id="insert_into_artist_appearances_by_date",
        conn_id="pg_conn",
        sql = '''
            INSERT INTO dm.artist_appearances_by_date
                SELECT date, country_name, artist_name, COUNT(*)
                FROM dds.fact_daily_top_100
                    JOIN dds.dim_artist USING(artist_id)
                    JOIN dds.dim_country USING(country_id)
                WHERE date = %(date)s   
                GROUP BY date, country_name, artist_name
        ''',
        parameters={"date": DATE}
    )

    insert_into_expected_artist_royalties_by_date = SQLExecuteQueryOperator(
        task_id="insert_into_expected_artist_royalties_by_date",
        conn_id="pg_conn",
        sql = '''
            INSERT INTO dm.expected_artist_royalties_by_date
                SELECT date, artist_name, ROUND(SUM(listeners_count) * 0.003, 2) AS royalties 
                FROM dds.fact_daily_top_100 fdt
                    JOIN dds.dim_artist da USING(artist_id)
                WHERE date = %(date)s
                GROUP BY date, artist_name	
                ORDER BY date, royalties DESC
        ''',
        parameters={"date": DATE}
    )

    end = EmptyOperator(
        task_id="end"
    )    

    start >> sensor_on_dds >> insert_into_avg_song_duration_by_country >> insert_into_artist_appearances_by_date >> insert_into_expected_artist_royalties_by_date >> end