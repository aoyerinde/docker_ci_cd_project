import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from shared_functions.utilities_dbt import dbt
from airflow.operators import TriggerDagRunOperator
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2021, 3, 30),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="dbt_product_weekly",
    default_args=default_args,
    schedule_interval = '0 9 * * MON',
    catchup = False
)


dbt_product_weekly_task = BashOperator(
    task_id='dbt_product_weekly_task',
    bash_command=dbt('run', 'tag:orchestration_product_weekly'),
    dag=dag
)



dbt_product_weekly_task
