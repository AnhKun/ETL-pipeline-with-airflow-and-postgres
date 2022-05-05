--create database airline
DROP SCHEMA IF EXISTS airline;
CREATE SCHEMA airline;

--drop tables if exists in the database
DROP TABLE IF EXISTS airline_dim;
DROP TABLE IF EXISTS cancel_dim;
DROP TABLE IF EXISTS delay_dim;
DROP TABLE IF EXISTS distance_dim;
DROP TABLE IF EXISTS port_loc_dim;
DROP TABLE IF EXISTS state_dim;

--create tables if not exist in the database
CREATE TABLE IF NOT EXISTS airline_dim (
    c_airline varchar(2) primary key,
    airline_name varchar(100)
);

CREATE TABLE IF NOT EXISTS cancel_dim (
    c_cancel varchar(1) primary key,
    cancel_des varchar 
);

CREATE TABLE IF NOT EXISTS delay_dim (
    delay_group varchar(4) primary key,
    time_range_minute varchar(150)
);

CREATE TABLE IF NOT EXISTS distance_dim (
    distance_group varchar(4) primary key,
    distance_range_mile varchar(150)
);

CREATE TABLE IF NOT EXISTS port_loc_dim (
    c_port varchar(5) primary key,
    city_name varchar(50),
    c_state varchar(2)
);

CREATE TABLE IF NOT EXISTS state_dim (
    c_state varchar(2) primary key,
    state_name varchar(50)
);

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
);
