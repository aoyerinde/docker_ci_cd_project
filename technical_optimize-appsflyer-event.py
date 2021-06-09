import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 5, 20),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="technical_optimize-appsflyer-event",
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup = False
)

db_technical_optimize_appsflyer_event = DatabricksSubmitRunOperator(
    task_id='db_technical_optimize-appsflyer-event',
    timeout_seconds=60*240,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/technical/optimize-appsflyer-event'),

    new_cluster=generate_cluster(num_workers=4),

    dag=dag
)
