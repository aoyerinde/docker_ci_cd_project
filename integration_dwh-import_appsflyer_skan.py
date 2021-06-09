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
    'start_date': datetime(2021, 2, 21),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id = "integration_appsflyer_skan",
    default_args = default_args_integration,
    schedule_interval = '0 6,8,15 * * *',
    catchup=False
)

db_integration_appsflyer_skan = DatabricksSubmitRunOperator(
    task_id='db_integration_appsflyer_skan',
    timeout_seconds = 120*60, # Timeout in seconds: 2 hours
    databricks_retry_delay = 300, # Time between retries in seconds
    databricks_retry_limit = 3, # Retry limit

    # Defines the Databricks notebook to run. ds and tomorrwo_ds are provided automatically
    #idfa skad data is only available after 68 hours after install, see documentation here :
    # https://support.appsflyer.com/hc/en-us/articles/360000310629#skadnetwork-8
    #if changed, the numbers below can be adjusted.

    notebook_task=generate_notebook_task('/production/jobs/integration/appsflyer_idfa_skad',
                                        parameters={
                                            'START_DATE': '{{  macros.ds_add(ds, -5) }}',
                                            'END_DATE': '{{  macros.ds_add(ds, -2) }}',
                                            'DESTINATION_TABLE': 'KG_DWH.RAW_MARKETING.APPSFLYER_SKAN',
                                            }
    ),
    # Defines the Databricks cluster the job should run on.
    new_cluster=generate_cluster(),

    dag = dag
)

db_integration_appsflyer_skan
