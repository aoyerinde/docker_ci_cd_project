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
    'start_date': datetime(2020, 7, 8),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="integration_game-design",
    default_args=default_args,
    schedule_interval='@daily', # '@daily', '@hourly', '0 0 * * *' (cron expression)
)

db_game_design_imt_mine_description = DatabricksSubmitRunOperator(
    task_id='game-design-imt_mine-description',
    timeout_seconds=60*60, # Timeout in seconds
    databricks_retry_delay=60, # Time between retries in seconds
    databricks_retry_limit=3, # Retry limit

    # Defines the Databricks notebook to run. ds and tomorrwo_ds are provided automatically
    notebook_task=generate_notebook_task('/production/jobs/integration/game-design-imt_mine-description'),
    # Defines the Databricks cluster the job should run on.
    new_cluster=generate_cluster(),

    dag=dag
)

# tasks definition for Snowflake RAW to L1
sf_rawtol1_imt_mine_description= SnowflakeOperator(
    task_id="sf_rawtol1_imt_mine_description",
    sql=prepare_sql_file("game_design/imt_mine_description.sql",
        params = {
            'TARGET_TABLE': 'L1_GAMEDATA.IMT_MINE_DESCRIPTION',
            'SOURCE_TABLE': 'RAW_GAMEDATA.IMT_MINE_DESCRIPTION',
                    }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

# Setting the dependencies of the individual tasks.
db_game_design_imt_mine_description >> sf_rawtol1_imt_mine_description
