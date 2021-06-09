import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 4, 1),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="cluster-management_snowflake-looker-pdt-cluster-downscale",
    default_args=default_args,
    schedule_interval='0 10 * * *',
    catchup = False
)

db_cluster_management_snowflake_looker_pdt_cluster_downscale = DatabricksSubmitRunOperator(
    task_id='db_cluster_management_snowflake_looker_pdt_cluster_downscale',
    timeout_seconds=60*30,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/cluster-management/snowflake-looker-pdt-cluster-downscale'),

    new_cluster=generate_cluster(),

    dag=dag
)
