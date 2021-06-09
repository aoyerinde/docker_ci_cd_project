import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 6, 25),
    'on_failure_callback': trigger_slack_alert,
    'retries': 3,
}

dag = DAG(
    dag_id="integration_game-build-pipeline_build-health",
    default_args=default_args,
    schedule_interval = '0 3,5 * * *', # daily at 3AM, 5AM  UTC,
    catchup =False
)

db_game_build_pipeline_build_health = DatabricksSubmitRunOperator(
    task_id='db_game-build-pipeline_build-health',
    timeout_seconds=30*60,
    databricks_retry_delay=30,
    databricks_retry_limit=3, 
    
    notebook_task=generate_notebook_task('/production/jobs/integration/game-build-pipeline_build-health'),
    new_cluster=generate_cluster(),
    dag=dag
)

sf_rawtol1_game_build_pipeline_build_health = SnowflakeOperator(
    task_id="sf_rawtol1_game_build_pipeline_build_health",
    sql=prepare_sql_file("integration_game-build-pipeline_build-health/rawtol1_game_build_pipeline_build_health.sql"),
    autocommit=False,
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

db_game_build_pipeline_build_health >> sf_rawtol1_game_build_pipeline_build_health

