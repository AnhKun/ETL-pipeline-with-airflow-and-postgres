import pandas as pd
import psycopg2
from tasks import config

# insert into tables
airline_dim_insert = """
INSERT INTO airline_dim (
    c_airline,
    airline_name 
)
VALUES (%s, %s)
"""

cancel_dim_insert = """
INSERT INTO cancel_dim (
    c_cancel,
    cancel_des 
)
VALUES (%s, %s)
"""

delay_dim_insert = """
INSERT INTO delay_dim (
    delay_group,
    time_range_minute
)
VALUES (%s, %s)
"""

distance_dim_insert = """
INSERT INTO distance_dim (
    distance_group,
    distance_range_mile
)
VALUES (%s, %s)
"""

port_loc_dim_insert = """
INSERT INTO port_loc_dim (
    c_port,
    city_name,
    c_state
)
VALUES (%s, %s, %s)
"""

state_dim_insert = """
INSERT INTO state_dim (
    c_state,
    state_name
)
VALUES (%s, %s)
"""

airline_fact_insert = """
INSERT INTO airline_fact (
    flight_id,
    flight_date,
    c_airline,
    flight_num,
    c_aircraft,
    origin,
    dest,
    schedule_dep_time,
    actual_dep_time,
    dep_delay_group,
    schedule_arr_time,
    actual_arr_time,
    arr_delay_group,
    distance_group,
    c_cancel 
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def process_data(cur, conn):
    """
    Function: insert all records to the postgres database
    Parameter: 
        - cur: cursor of postgres server
        - conn: connection to postgres server
    """

    # create the dataframe for each dataset
    airline_df = pd.read_csv('/usr/local/airflow/data_airflow/airline.csv')
    cancel_df = pd.read_csv('/usr/local/airflow/data_airflow/cancellation.csv')
    delay_df = pd.read_csv('/usr/local/airflow/data_airflow/delay_group.csv')
    distance_df = pd.read_csv('/usr/local/airflow/data_airflow/distance_group.csv')
    port_loc_df = pd.read_csv('/usr/local/airflow/data_airflow/port_loc.csv')
    state_df = pd.read_csv('/usr/local/airflow/data_airflow/states.csv')

    fact_df = pd.read_csv('/usr/local/airflow/data_airflow/fact.csv')

    # save value into postgress sever
    for _, row in airline_df.iterrows():
        cur.execute(airline_dim_insert, row)
        conn.commit()

    for _, row in cancel_df.iterrows():
        cur.execute(cancel_dim_insert, row)
        conn.commit()

    for _, row in delay_df.iterrows():
        cur.execute(delay_dim_insert, row)
        conn.commit()

    for _, row in distance_df.iterrows():
        cur.execute(distance_dim_insert, row)
        conn.commit()

    for _, row in port_loc_df.iterrows():
        cur.execute(port_loc_dim_insert, row)
        conn.commit()

    for _, row in state_df.iterrows():
        cur.execute(state_dim_insert, row)
        conn.commit()

    for _, row in fact_df.iterrows():
        cur.execute(airline_fact_insert, row)
        conn.commit()


def main():
    """
    Connect to the postgres server
    Insert all records to postgres database
    Close connection
    """

    # connect to airline database
    conn = psycopg2.connect(database='postgres', host='postgres', port='5432', user=config.user, password=config.password)
    cur = conn.cursor()

    process_data(cur, conn)

    conn.close()
