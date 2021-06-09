import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2021, 4, 26),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
    'sla': timedelta(hours=2)
}

dag = DAG(
    dag_id="integration_google_adlevel_metrics",
    default_args=default_args,
    schedule_interval = '0 3,4,5 * * *', # daily at 3AM, 4AM,  and 5AM UTC
    catchup=False
)


db_google_adlevel_metrics= DatabricksSubmitRunOperator(
    task_id='db_google_adlevel_metrics',
    timeout_seconds=120*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/google_adlevel_metrics',
            parameters = {
                'START_DATE': '{{  macros.ds_add(ds, -7) }}',
                'END_DATE': '{{  macros.ds_add(ds, +0) }}',
                'DESTINATION_TABLE': 'KG_DWH.RAW_MARKETING.GOOGLE_ADLEVEL_METRICS',
            }),
    new_cluster=generate_cluster(
        num_workers=1,
        other={
                "spark_version":
                        "8.1.x-scala2.12"
        }),
    dag=dag
)