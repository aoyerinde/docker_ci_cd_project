import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 4, 2),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="aggregation_data-quality_reference-data-recency",
    default_args=default_args,
    schedule_interval='30 4 * * *',
    catchup = False
)

db_aggregation_data_quality_reference_data_recency = DatabricksSubmitRunOperator(
    task_id='db_aggregation_data_quality_reference_data_recency',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/aggregation/data-quality_reference-data-recency'),

    new_cluster=generate_cluster(),

    dag=dag
)
