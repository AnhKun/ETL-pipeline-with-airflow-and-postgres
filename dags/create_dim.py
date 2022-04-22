import datetime
import numpy as np
import pandas as pd
import re

def lowercase_columns():
    df = pd.read_csv('/usr/local/airflow/data_airflow/jantojun2020.csv', low_memory=False)
    df.columns = df.columns.map(lambda x: x.lower())
    return df.to_csv('/usr/local/airflow/data_airflow/jantojun2020.csv', index=False)

def create_port_loc_df(path_csv):
    df = pd.read_csv(path_csv, low_memory=False)
    df = df[['origin', 'origin_city_name', 'origin_state_abr']].drop_duplicates()
    df['origin_city_name'] = df['origin_city_name'].map(lambda x: x.split(',')[0])
    return df.to_csv('/usr/local/airflow/data_airflow/port_loc.csv', index=False)

def create_states_ds(path_csv):
    df = pd.read_csv(path_csv, low_memory=False)
    df = df[['origin_state_abr', 'origin_state_nm']].drop_duplicates()
    return df.to_csv('/usr/local/airflow/data_airflow/states.csv', index=False)

def create_airline_df(path_txt):
        with open(path_txt) as f:
            content = f.readlines()
            content = [x.strip() for x in content]
            airline = content[10:20]
            splitted_airline = [c.split(":") for c in airline]
            c_airline = [x[0].replace("'","").strip() for x in splitted_airline]
            airline_name = [x[1].replace("'","").strip() for x in splitted_airline]
            airline_df = pd.DataFrame(zip(c_airline, airline_name), columns=['c_airline', 'airline_name'])
            return airline_df.to_csv("/usr/local/airflow/data_airflow/airline.csv", index=False)

def create_distance_group_ds():
    data = []
    for i in range(23):
        data.append([i, "{} <= distance < {}".format(i * 250, (i + 1) * 250)])
        
    df = pd.DataFrame(data=data, columns=['distance_group', 'distance_range(miles)'])
    return df.to_csv('/usr/local/airflow/data_airflow/distance_group.csv', index=False)   

def create_cancellation_ds(path_txt):
        """
        Function: Generate and create Cancelation_code table:
        param: Path of datafile
        input: .txt file
        output: cancel.csv file stored in data folder
        """
        with open(path_txt) as f:
            content = f.readlines()
            content = [x.strip() for x in content]
            cancel = [re.search('\(([^)]+)', content[49]).group(1)][0].split(",")
            splitted_cancel = [c.split(":") for c in cancel]
            c_cancel = [x[0].replace("'","").strip() for x in splitted_cancel]
            cancel_des= [x[1].replace("'","").strip() for x in splitted_cancel]
            c_cancel.append('O')
            c_cancel = map(str, c_cancel)
            cancel_des.append('Non-cancel')
            cancel_df = pd.DataFrame({"c_cancel" : c_cancel, "cancel_des": cancel_des})
            return cancel_df.to_csv("/usr/local/airflow/data_airflow/cancellation.csv", index=False)

def create_delay_group_ds():
        """
        function
        """
        data = []
        for i in range(-1,188):
            if i == -1:
                data.append([-1,"Early"])
            elif i == 0:
                data.append([0,"On Time"])
            else:
                data.append([i, "{} <= delay time < {}".format(i * 15, (i + 1) * 15)])

        df = pd.DataFrame(data=data, columns=['delay_group', 'delay_time_range(minutes)'])
        return df.to_csv('/usr/local/airflow/data_airflow/delay_group.csv', index=False)

def main():
    path_csv = '/usr/local/airflow/data_airflow/jantojun2020.csv'
    path_txt = '/usr/local/airflow/data_airflow/ColumnDescriptions.txt'

    lowercase_columns()
    create_port_loc_df(path_csv)
    create_states_ds(path_csv)
    create_airline_df(path_txt)
    create_distance_group_ds()
    create_cancellation_ds(path_txt)
    create_delay_group_ds()

#if __name__ == '__main__':
#    main()