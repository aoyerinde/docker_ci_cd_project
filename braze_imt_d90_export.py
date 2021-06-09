import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert

default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2021, 1, 10),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
    'retry_delay': timedelta(seconds=600)
}

dag = DAG(
    dag_id="braze_imt_d90_export",
    default_args=default_args,
    schedule_interval = '0 5 * * *', 
    catchup=False
)

db_braze_imt_iap_d90_export = DatabricksSubmitRunOperator(
    task_id='db_braze_imt_iap_d90_export',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/automation/braze_imt_d90_export',
        parameters={
            'START_DATE': '{{  ds }}',
            'END_DATE': '{{  macros.ds_add(ds, +1) }}',
            'EVENT': 'IAP'
        }),
    new_cluster=generate_cluster(num_workers = 2),
    dag=dag
)


db_braze_imt_ad_d90_export = DatabricksSubmitRunOperator(
    task_id='db_braze_imt_ad_d90_export',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/automation/braze_imt_d90_export',
        parameters={
            'START_DATE': '{{  ds }}',
            'END_DATE': '{{  macros.ds_add(ds, +1) }}',
            'EVENT': 'AD'
        }),
    new_cluster=generate_cluster(num_workers = 2),
    dag=dag
)

db_braze_imt_payer_d90_export = DatabricksSubmitRunOperator(
    task_id='db_braze_imt_payer_d90_export',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/automation/braze_imt_payer_d90_export',
        parameters={
            'START_DATE': '{{  ds }}',
            'END_DATE': '{{  macros.ds_add(ds, +1) }}'
        }),
    new_cluster=generate_cluster(num_workers = 2),
    dag=dag
)