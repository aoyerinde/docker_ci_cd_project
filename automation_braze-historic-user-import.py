import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 6, 9),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="automation_braze-historic-user-import",
    default_args=default_args,
    schedule_interval = '0 12 * * *',
)

db_braze_historic_user_import = DatabricksSubmitRunOperator(
    task_id='db_braze_historic_user_import',
    timeout_seconds=150*60,
    databricks_retry_delay=60,
    databricks_retry_limit=1,

    notebook_task=generate_notebook_task('/production/jobs/automation/braze_historic_user_import'),
    new_cluster=generate_cluster(),
    dag=dag
)
