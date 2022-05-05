# ETL processing with airflow and postgreSQL
The purpose of this project is to give a demo of an orchestration tool for building an ETL pipeline, where the datastore is a PostgreSQL database.
## 1. Dataset
This dataset is about the airline delay and cancellation in US in 2020.

For more information, it can be found [here](https://www.kaggle.com/datasets/akulbahl/covid19-airline-flight-delays-and-cancellations)

## 2. Environment
In this project, airflow was deployed by customizing the docker-compose which was inherited from [puckel project](https://github.com/puckel/docker-airflow).

To deploy the environment, run the following command: 
<br>`docker-compose -f docker-compose-LocalExecutor.yml up -d`
<br>The postgres connection on airflow was configured like the figure below
![postgres_connection](https://github.com/AnhKun/ETL-pipeline-with-airflow-and-postgres/blob/master/img/Capture3.PNG)


## 3. Dags architechture
![dag_architechture](https://github.com/AnhKun/ETL-pipeline-with-airflow-and-postgres/blob/master/img/Capture4.PNG)

## 4. Result
![access_postgres](https://github.com/AnhKun/ETL-pipeline-with-airflow-and-postgres/blob/master/img/capture1.PNG)

![connect_database](https://github.com/AnhKun/ETL-pipeline-with-airflow-and-postgres/blob/master/img/Capture2.PNG)

![total_rows_fact_table](https://github.com/AnhKun/ETL-pipeline-with-airflow-and-postgres/blob/master/img/Capture5.PNG)