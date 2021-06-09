import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2021, 4, 22),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="automation_facebook-tokens",
    default_args=default_args,
    schedule_interval = '0 3 * * MON,THU', #Mon/THU at 3am UTC
    catchup= False
)

db_automation_facebook_tokens = DatabricksSubmitRunOperator(
    task_id='db_automation_facebook_tokens',
    timeout_seconds=180*60,
    databricks_retry_delay=60,
    databricks_retry_limit=1,

    notebook_task=generate_notebook_task('/production/jobs/automation/facebook_tokens-automation'),
    new_cluster=generate_cluster(),
    dag=dag
)



dummy_final_task = DummyOperator(
    task_id='dummy_final_task',
    trigger_rule='all_done',
    dag=dag
)

db_automation_facebook_tokens >>  dummy_final_task
