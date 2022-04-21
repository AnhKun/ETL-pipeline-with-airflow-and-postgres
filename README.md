# ETL processing with airflow and postgreSQL
The purpose of this project is to give a demo of orchestration tool in building an ETL pipeline, where the datastore is PostgreSQL database.
## 1. Dataset
This dataset is about the airline delay and cancellation in US in 2020.

For more information, it can be found [here](https://www.kaggle.com/datasets/akulbahl/covid19-airline-flight-delays-and-cancellations)

## 2. Environment
In this project, airflow was deployed by customizing the docker-compose which was inherited from [puckel project](https://github.com/puckel/docker-airflow).
