import pandas as pd
import numpy as np
from datetime import datetime

def lowercase_columns():
    df = pd.read_csv('/usr/local/airflow/data_airflow/jantojun2020.csv', low_memory=False)
    df.columns = df.columns.map(lambda x: x.lower())
    return df.to_csv('/usr/local/airflow/data_airflow/jantojun2020.csv', index=False)

def create_fact():
    data = pd.read_csv('/usr/local/airflow/data_airflow/jantojun2020.csv', low_memory=False)
    data = data[['fl_date', 'mkt_unique_carrier', 'mkt_carrier_fl_num', 'tail_num', 'origin', 'dest',\
            'crs_dep_time', 'dep_time', 'dep_delay_group', 'crs_arr_time', 'arr_time', 'arr_delay_group',\
            'distance_group', 'cancellation_code']]
    data['fl_date'] = pd.to_datetime(data['fl_date'])
    data['cancellation_code'].fillna('O', inplace=True)

    data['dep_time'].fillna('NA', inplace=True)
    data['arr_time'].fillna('NA', inplace=True)
    data['dep_delay_group'].fillna('NA', inplace=True)
    data['arr_delay_group'].fillna('NA', inplace=True)

    return data.to_csv('/usr/local/airflow/data_airflow/fact.csv')

def main():
    lowercase_columns()
    create_fact()

