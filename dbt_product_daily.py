import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from shared_functions.utilities_dbt import dbt
from airflow.operators import TriggerDagRunOperator
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 5, 6),
    'on_failure_callback': trigger_slack_alert
}

dag = DAG(
    dag_id="dbt_product_daily",
    default_args=default_args,
    schedule_interval = '0 5 * * *',
    catchup = False
)

dbt_clientsdk_l1_availability_test = BashOperator(
    task_id='dbt_clientsdk_availability_test',
    bash_command=dbt('test', 'tag:validation_product_daily'),
    dag=dag
)

dbt_product_daily_run = BashOperator(
    task_id='dbt_product_daily_run',
    bash_command=dbt('run', 'tag:orchestration_product_daily --exclude tag:hourly'),
    dag=dag
)

dbt_product_daily_test = BashOperator(
    task_id='dbt_product_daily_test',
    bash_command=dbt('test', 'tag:orchestration_product_daily --exclude tag:hourly'),
    dag=dag
)

# this task triggers another Dag - must run when the other tasks are complete
dag_trigger_integration_telescope = TriggerDagRunOperator(
    task_id="dag_trigger_integration_telescope",
    trigger_dag_id="integration_telescope-client-sdk-user-agg",
    dag=dag,
)

dbt_dag_freshness_run = BashOperator(
    task_id='dbt_dag_freshness_run',
    bash_command=dbt('run', f"dag_freshness --vars 'dag_name: {dag.dag_id}'"),
    dag=dag
)

dbt_clientsdk_l1_availability_test >> dbt_product_daily_run >> [dbt_product_daily_test, dag_trigger_integration_telescope]
dbt_product_daily_test >> dbt_dag_freshness_run