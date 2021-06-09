import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from shared_functions.utilities_dbt import dbt
from shared_functions.utilities_slack import trigger_slack_alert

default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2021, 2, 12),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="aggregation_marketing_bi-hourly",
    default_args=default_args,
    schedule_interval='0 */2 * * *',
    catchup=False
)

dbt_marketing_hourly_run = BashOperator(
    task_id='dbt_marketing_hourly_run',
    bash_command=dbt('run', 'tag:orchestration_marketing_hourly'),
    dag=dag
)

dbt_marketing_hourly_test = BashOperator(
    task_id='dbt_marketing_hourly_test',
    bash_command=dbt('test', 'tag:orchestration_marketing_hourly'),
    dag=dag
)

dbt_marketing_hourly_run >> dbt_marketing_hourly_test
