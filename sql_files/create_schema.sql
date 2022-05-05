--create database airline
DROP DATABASE IF EXISTS airline;
CREATE DATABASE airline WITH ENCODING 'utf8' TEMPLATE template0

--drop tables if exists in the database
airline_dim_drop = 'DROP TABLE IF EXISTS airline_dim;'
cancel_dim_drop = 'DROP TABLE IF EXISTS cancel_dim;'
delay_dim_drop = 'DROP TABLE IF EXISTS delay_dim;'
distance_dim_drop = 'DROP TABLE IF EXISTS distance_dim;'
port_loc_dim_drop = 'DROP TABLE IF EXISTS port_loc_dim;'
state_dim_drop = 'DROP TABLE IF EXISTS state_dim;'

--create tables if not exist in the database
airline_dim_create = """
CREATE TABLE IF NOT EXISTS airline_dim (
    c_airline varchar(2) primary key,
    airline_name varchar(100)
)
"""

cancel_dim_create = """
CREATE TABLE IF NOT EXISTS cancel_dim (
    c_cancel varchar(1) primary key,
    cancel_des varchar 
)
"""

delay_dim_create = """
CREATE TABLE IF NOT EXISTS delay_dim (
    delay_group varchar(4) primary key,
    time_range_minute varchar(150)
)
"""

distance_dim_create = """
CREATE TABLE IF NOT EXISTS distance_dim (
    distance_group varchar(4) primary key,
    distance_range_mile varchar(150)
)
"""

port_loc_dim_create = """
CREATE TABLE IF NOT EXISTS port_loc_dim (
    c_port varchar(5) primary key,
    city_name varchar(50),
    c_state varchar(2)
)
"""

state_dim_create = """
CREATE TABLE IF NOT EXISTS state_dim (
    c_state varchar(2) primary key,
    state_name varchar(50)
)
"""

airline_fact_create = """
CREATE TABLE IF NOT EXISTS airline_fact (
    flight_id int primary key,
    flight_date date,
    c_airline varchar(2),
    flight_num varchar(7),
    c_aircraft varchar(7),
    origin varchar(50),
    dest varchar(50),
    schedule_dep_time varchar(10),
    actual_dep_time varchar(10),
    dep_delay_group varchar(4),
    schedule_arr_time varchar(10),
    actual_arr_time varchar(10),
    arr_delay_group varchar(4),
    distance_group varchar(4),
    c_cancel varchar(1) 
)
"""

--insert into tables
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

--query list
create_tables_query = [airline_dim_create, cancel_dim_create, delay_dim_create, distance_dim_create, port_loc_dim_create, state_dim_create, airline_fact_create]

drop_tables_query = [airline_dim_drop, cancel_dim_drop, delay_dim_drop, distance_dim_drop, port_loc_dim_drop, state_dim_drop, airline_fact_drop]

schema = [airline_dim_drop, cancel_dim_drop, delay_dim_drop, 
        distance_dim_drop, port_loc_dim_drop, state_dim_drop, airline_fact_drop,
        airline_dim_create, cancel_dim_create, delay_dim_create, 
        distance_dim_create, port_loc_dim_create, state_dim_create, airline_fact_create]