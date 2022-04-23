from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from tasks import create_dim, create_fact
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
    t1 = BashOperator(
        task_id='check_files_exists',
        bash_command= """
            [[ -f "/usr/local/airflow/data_airflow/jantojun2020.csv" ]] && echo "This file exists!"
            [[ -f "/usr/local/airflow/data_airflow/ColumnDescriptions.txt" ]] && echo "This file exists!"
        """
    )

    t2 = PythonOperator(
        task_id='create_dim_ds',
        python_callable=create_dim.main
    )

    t3 = PythonOperator(
        task_id='create_fact_ds',
        python_callable=create_fact.main
    )




    t1 >> [t2, t3]
