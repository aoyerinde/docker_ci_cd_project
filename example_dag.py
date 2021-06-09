import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 2, 18),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="example_dag",
    default_args=default_args,
    schedule_interval='@daily', # '@daily', '@hourly', '0 0 * * *' (cron expression)
)

db_airflow_called_notebook = DatabricksSubmitRunOperator(
    task_id='db_airflow_called_notebook',
    timeout_seconds=60*60, # Timeout in seconds
    databricks_retry_delay=60, # Time between retries in seconds
    databricks_retry_limit=3, # Retry limit

    # Defines the Databricks notebook to run. ds and tomorrwo_ds are provided automatically
    notebook_task=generate_notebook_task('/Shared/example/airflow_called_notebook'),
    # Defines the Databricks cluster the job should run on.
    new_cluster=generate_cluster(),

    dag=dag
)

sf_example_one = SnowflakeOperator(
    task_id="sf_example_one",
    sql=prepare_sql_file("example_dag/example_one.sql"), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

sf_example_two = SnowflakeOperator(
    task_id="sf_example_two",
    sql=prepare_sql_file("example_dag/example_two.sql"), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

# Setting the dependencies of the individual tasks.
db_airflow_called_notebook >> [sf_example_one, sf_example_two]
