import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.bash_operator import BashOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_dbt import dbt
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2021, 3, 25),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1
}

dag = DAG(
    dag_id="dwh-import_clientsdk_event_l1",
    default_args=default_args,
    schedule_interval='0 */4 * * *',
    catchup=False
)

db_adls_clientsdk_health_check = DatabricksSubmitRunOperator(
    task_id='db_adls_clientsdk_health_check',
    timeout_seconds=180*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/monitoring/clientsdk_health_check'),
    new_cluster=generate_cluster(num_workers = 2),

    dag=dag
)

db_client_sdk_event_raw = DatabricksSubmitRunOperator(
    task_id='db_client_sdk_event_raw',
    timeout_seconds=180*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/dwh-import/client-sdk_event_raw'),
    new_cluster=generate_cluster(num_workers = 2),

    dag=dag
)

dbt_clientsdk_raw_health_checks = BashOperator(
    task_id='dbt_clientsdk_raw_health_checks',
    bash_command=dbt('run', 'clientsdk.clientsdk_raw_health_checks'),
    dag=dag
)

dbt_clientsdk_l1_integration = BashOperator(
    task_id='dbt_clientsdk_l1_integration',
    bash_command=dbt('run', 'clientsdk.clientsdk_l1_integration'),
    dag=dag
)

dbt_clientsdk_l1_health_checks = BashOperator(
    task_id='dbt_clientsdk_l1_health_checks',
    bash_command=dbt('run', 'clientsdk.clientsdk_l1_health_checks'),
    dag=dag
)

dbt_clientsdk_l1_integration_test = BashOperator(
    task_id='dbt_clientsdk_l1_integration_test',
    bash_command=dbt('test', 'clientsdk.clientsdk_l1_integration'),
    dag=dag
)

db_adls_clientsdk_health_check  >> db_client_sdk_event_raw \
>> dbt_clientsdk_raw_health_checks >> dbt_clientsdk_l1_integration \
>> dbt_clientsdk_l1_health_checks >> dbt_clientsdk_l1_integration_test
