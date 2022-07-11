import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from spotify_pipe import etl_main

 
defaults = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 5),
    'email': ['EMAIL-FOR-NOTIFICATIONS'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 1)   
}

dag = DAG(
    'songs-dag',
    default_args = defaults,
    description = 'Spotify songs ETL dag',
    schedule_interval = timedelta(days = 1)
)

    

run_etl = PythonOperator(
    task_id = 'spotify_etl',
    python_callable = etl_main(),
    dag = dag
)

