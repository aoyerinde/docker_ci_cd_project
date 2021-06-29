import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from shared_functions.utilities_dbt import dbt


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 4, 23),
    'on_failure_callback': trigger_slack_alert,
    'retries': 0,
}

dag = DAG(
    dag_id="example_dbt",
    default_args=default_args,
    schedule_interval = None
)

dbt_task = BashOperator(
    task_id='dbt_task',
    bash_command=dbt('run', 'example-project'),
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=dbt('test', 'example-project'),
    dag=dag
)

dbt_task >> dbt_test
