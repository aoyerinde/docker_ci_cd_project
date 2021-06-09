import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.bash_operator import BashOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_dbt import dbt
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert

default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2021, 5, 4),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="dwh-import_appsflyer_event",
    default_args=default_args,
    schedule_interval='@hourly', # '@daily', '@hourly', '0 0 * * *' (cron expression)
    catchup = True,
    max_active_runs = 1

)

db_adls_appsflyer_health_check = DatabricksSubmitRunOperator(
    task_id='db_adls_appsflyer_health_check',
    timeout_seconds=180*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/monitoring/appsflyer_health_check'),
    new_cluster=generate_cluster(num_workers = 2),

    dag=dag
)


# tasks definition for ADLS to Snowflake RAW integration
db_appsflyer_event_raw = DatabricksSubmitRunOperator(
    task_id = 'db_appsflyer_event_raw',
    timeout_seconds = 50*60,
    databricks_retry_delay = 60,
    databricks_retry_limit = 3,

    notebook_task = generate_notebook_task(
        '/production/jobs/dwh-import/appsflyer-event_pushapi-v2-raw',
        parameters = {
            'END_TIME': '{{ (execution_date + macros.timedelta(hours=1)).to_datetime_string() }}',
            'START_TIME_QUERY': 'SELECT START_TIME FROM DATA_QUALITY_METRIC.APPSFLYER_INTEGRATION_SLICESTART_HOUR'
        }),
    new_cluster = generate_cluster(
        num_workers = 6,
        node_type_id = 'Standard_F4s',
        other = {'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
        ),
    dag = dag
)


dbt_appflyer_raw_health_checks = BashOperator(
    task_id='dbt_appflyer_raw_health_checks',
    bash_command=dbt('run', 'appsflyer_event.appsflyer_raw_health_checks'),
    dag=dag
)



dbt_l1_integration = BashOperator(
    task_id='dbt_l1_integration',
    bash_command=dbt('run', 'tag:orchestration_l1_integration_hourly'),
    dag=dag
)

dbt_appflyer_l1_health_checks = BashOperator(
    task_id='dbt_appflyer_l1_health_checks',
    bash_command=dbt('run', 'appsflyer_event.appsflyer_l1_health_checks'),
    dag=dag
)


dbt_l1_integration_test = BashOperator(
    task_id='dbt_l1_integration_test',
    bash_command=dbt('test', 'orchestration_l1_integration_hourly'),
    dag=dag
)

sf_l1_appsflyer_game_kpis_hourly = SnowflakeOperator(
    task_id="sf_l1_game_kpis_hourly",
    sql=prepare_sql_file("dwh-import_appsflyer_event/data_quality/l1_appsflyer_game_kpis_hourly.sql"), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

db_adls_appsflyer_health_check >> db_appsflyer_event_raw >> dbt_appflyer_raw_health_checks >> dbt_l1_integration >> dbt_appflyer_l1_health_checks >> dbt_l1_integration_test \
>> sf_l1_appsflyer_game_kpis_hourly
