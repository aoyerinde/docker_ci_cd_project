import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 7, 28),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="aggregation_clientsdk_qa-deviceid-list",
    default_args=default_args,
    schedule_interval = '0 */3 * * *', # daily at 8:30AM UTC,
)


sf_insert_new_qa_device_ids = SnowflakeOperator(
    task_id="sf_insert_new_qa_device_ids",
    sql=prepare_sql_file("aggregation_clientsdk_qa-deviceid-list/insert_new_qa_device_ids.sql"),
    autocommit = False,
    dag = dag
)

sf_insert_new_qa_playfab_ids = SnowflakeOperator(
    task_id="sf_insert_new_qa_playfab_ids",
    sql=prepare_sql_file("aggregation_clientsdk_qa-deviceid-list/insert_new_qa_playfab_ids.sql"),
    autocommit = False,
    dag = dag
)

db_integrate_clientsdk_qa_device_list = DatabricksSubmitRunOperator(
    task_id='db_integrate_clientsdk_qa-device-list',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task(
        '/production/jobs/aggregation/client-sdk_qa-device-list',
        parameters = {
            'START_DATE': '{{ macros.ds_add(ds, -2) }}',
            'END_DATE': '{{ macros.ds_add(ds, 2) }}'
        }),
    new_cluster=generate_cluster(),
    dag=dag
)

[sf_insert_new_qa_device_ids , sf_insert_new_qa_playfab_ids] >> db_integrate_clientsdk_qa_device_list
