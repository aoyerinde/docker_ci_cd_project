from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

dag_params = {
    'dag_id': 'PostgresOperator_dag',
    'start_date': datetime(2019, 10, 7),
    'schedule_interval': None
}

with DAG(**dag_params) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        sql='''
        CREATE TABLE new_table(
            custom_id integer NOT NULL, timestamp TIMESTAMP NOT NULL, user_id VARCHAR (50) NOT NULL
            );
         ''',
    )