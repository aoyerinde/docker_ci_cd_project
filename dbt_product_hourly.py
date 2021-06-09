import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from shared_functions.utilities_dbt import dbt
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 5, 6),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="dbt_product_hourly",
    default_args=default_args,
    schedule_interval = '@hourly',
    catchup = False
)

dbt_product_hourly_task = BashOperator(
    task_id='dbt_product_hourly_task',
    bash_command=dbt('run', 'tag:orchestration_product_hourly'),
    dag=dag
)

dbt_product_hourly_test = BashOperator(
    task_id='dbt_product_hourly_test',
    bash_command=dbt('test', 'tag:orchestration_product_hourly'),
    dag=dag
)

dbt_product_hourly_task >> dbt_product_hourly_test