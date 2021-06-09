import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 4, 1),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="integration_applovin-max-impressions",
    default_args=default_args,
    schedule_interval = '0 7 * * *',
    catchup = False
)

db_integration_applovin_max_impressions = DatabricksSubmitRunOperator(
    task_id='db_integration_applovin_max_impressions',
    timeout_seconds=45*60, 
    databricks_retry_delay=60, 
    databricks_retry_limit=2, 
    
    notebook_task=generate_notebook_task('/production/jobs/integration/applovin_max-impressions'),
    new_cluster=generate_cluster(),
    dag=dag
)
