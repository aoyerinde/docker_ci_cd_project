import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from shared_functions.utilities_dbt import dbt
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 8, 18),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="dbt_documentation",
    default_args=default_args,
    schedule_interval = '@hourly',
    catchup = False
)

vm_dbt_webserver_prod_ip = '40.78.30.59'

dbt_generate_documentation = BashOperator(
    task_id='dbt_generate_documentation',
    bash_command=dbt('docs generate')\
                 + 'scp -P 2222 target/index.html dbtuser@{ip}:./dbt-docs/;'.format(ip=vm_dbt_webserver_prod_ip)\
                 + 'scp -P 2222 target/*.json dbtuser@{ip}:./dbt-docs/;'.format(ip=vm_dbt_webserver_prod_ip),
    dag=dag
)
