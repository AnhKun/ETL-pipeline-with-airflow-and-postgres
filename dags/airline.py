from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime, timedelta
import create_dim
#from ..data_wrangling import main

default_args = {
    "owner": "anguyen",
    "start_date": datetime(2022, 4, 22),
    "retries": 2,
    "retry_delay": timedelta(seconds=5)
}

with DAG(
    'airline_ETL',
    default_args=default_args,
    description='A simple ETL pipeline',
    schedule_interval='@daily'
) as dag:

    create_dim_ds = PythonOperator(
        task_id='create_dim_ds',
        python_callable=create_dim.main
    )


    create_dim_ds
