import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator

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
    dag_id="automation_ironsource-bidding",
    default_args=default_args,
    schedule_interval = '15 8 * * TUE,FRI', # Mo/We/Fr at 8:15 UTC
    catchup= False
)

db_automation_ironsource_bidding_cpi = DatabricksSubmitRunOperator(
    task_id='db_automation_ironsource_bidding_cpi',
    timeout_seconds=180*60,
    databricks_retry_delay=60,
    databricks_retry_limit=1,

    notebook_task=generate_notebook_task('/production/jobs/automation/ironsource_bidding'),
    new_cluster=generate_cluster(),
    dag=dag
)

db_automation_ironsource_bidding_cpe = DatabricksSubmitRunOperator(
    task_id='db_automation_ironsource_bidding_cpe',
    timeout_seconds=180*60,
    databricks_retry_delay=60,
    databricks_retry_limit=1,

    notebook_task=generate_notebook_task('/production/jobs/automation/ironsource_bidding_cpe'),
    new_cluster=generate_cluster(),
    dag=dag
)

dummy_final_task = DummyOperator(
    task_id='dummy_final_task',
    trigger_rule='all_done',
    dag=dag
)

db_automation_ironsource_bidding_cpi >> db_automation_ironsource_bidding_cpe >> dummy_final_task
