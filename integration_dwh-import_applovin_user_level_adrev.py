import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.bash_operator import BashOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_dbt import dbt
from shared_functions.utilities_slack import trigger_slack_alert


default_args_integration = {
    'owner': 'Data Platform',
    'start_date': datetime(2021, 3, 30),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id = "integration_dwh-import_dbt_applovin_user-level-ad-rev",
    default_args = default_args_integration,
    schedule_interval = '0 4,7,8,10 * * *',
    catchup=False
)

db_integration_applovin_user_level_adrev_w_fb = DatabricksSubmitRunOperator(
    task_id='db_integration_applovin_user_level_adrev_w_fb',
    timeout_seconds = 120*60, # Timeout in seconds: 2 hours
    databricks_retry_delay = 300, # Time between retries in seconds
    databricks_retry_limit = 3, # Retry limit

    # Defines the Databricks notebook to run. ds and tomorrwo_ds are provided automatically
    notebook_task=generate_notebook_task('/production/jobs/integration/applovin_user-level-revenue-with-facebook-bidding',
                                        parameters={
                                            'START_DATE': '{{  macros.ds_add(ds, -2) }}',
                                            'END_DATE': '{{  macros.ds_add(ds, +1) }}',
                                            'APPS': '''com.fluffyfairygames.idleminertycoon,
                                                       com.kolibrigames.idlerestauranttycoon,
                                                       com.handsomeoldtree.idlefirefightertycoon,
                                                       com.fancyelephant.idlemergetycoon,
                                                       com.twindeerarts.idlepiratetycoon'''
                                            }
    ),
    # Defines the Databricks cluster the job should run on.
    new_cluster=generate_cluster(),

    dag = dag
)

dbt_userleveladrev_daily_run = BashOperator(
    task_id='dbt_userleveladrev_daily_run',
    bash_command=dbt('run', 'tag:orchestration_userleveladrev_daily'),
    dag=dag
)

dbt_userleveladrev_daily_test = BashOperator(
    task_id='dbt_userleveladrev_daily_test',
    bash_command=dbt('test', 'tag:orchestration_userleveladrev_daily'),
    dag=dag
)

db_integration_applovin_user_level_adrev_w_fb  >> \
dbt_userleveladrev_daily_run >> dbt_userleveladrev_daily_test
