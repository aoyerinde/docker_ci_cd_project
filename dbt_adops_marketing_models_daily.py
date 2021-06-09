import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from shared_functions.utilities_dbt import dbt
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2021, 1, 28),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="dbt_adops_marketing_models_daily",
    default_args=default_args,
    schedule_interval = '0 5,7 * * *',
    catchup = False
)

adops_marketing_models_daily_task = BashOperator(
    task_id='adops_marketing_models_daily_task',
    bash_command=dbt('run', 'tag:orchestration_adops_marketing_daily'),
    dag=dag
)

adops_marketing_models_daily_test= BashOperator(
    task_id='adops_marketing_models_daily_test',
    bash_command=dbt('test', 'tag:orchestration_adops_marketing_daily'),
    dag=dag
)

adops_marketing_models_daily_task  >> adops_marketing_models_daily_test
