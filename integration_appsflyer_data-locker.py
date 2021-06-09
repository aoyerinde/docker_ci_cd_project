import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.dummy_operator import DummyOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 3, 30),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="dwh-import_appsflyer_data-locker",
    default_args=default_args,
    schedule_interval='0 7 * * *',
    catchup=False,
    max_active_runs=4
)

aws_to_datalake_events = ["blocked_clicks", "blocked_inapps", "blocked_installs", "clicks"
    , "clicks_retargeting", "conversions_retargeting", "impressions", "inapps"
    , "inapps_retargeting", "installs", "sessions", "uninstalls"
    , "attributed_ad_revenue", "organic_ad_revenue"]
db_appsflyer_datalocker_aws_tasks = {}

for event in aws_to_datalake_events:
    db_appsflyer_datalocker_aws_tasks[event] = DatabricksSubmitRunOperator(
        task_id='db_appsflyer_data_locker_aws_' + event,
        timeout_seconds=60*60*6,
        databricks_retry_delay=60,
        databricks_retry_limit=3,

        notebook_task=generate_notebook_task('/production/jobs/integration/appsflyer_datalocker',
            parameters={
                'START_DATE': '{{  macros.ds_add(ds, -2) }}',
                'END_DATE': '{{  macros.ds_add(ds, +1) }}',
                'EVENT': event,
                'ENVIRONMENT': 'Production'
            }),
        new_cluster=generate_cluster(
            num_workers=2,
            other = {'spark_conf':{'spark.sql.autoBroadcastJoinThreshold': '-1'}}
            ),
        dag=dag
    )

datalake_to_raw_events = ["sessions", "attributed_ad_revenue", "organic_ad_revenue"]
db_appsflyer_datalocker_raw_tasks = {}

for event in datalake_to_raw_events:
    db_appsflyer_datalocker_raw_tasks[event] = DatabricksSubmitRunOperator(
        task_id='db_appsflyer_datalocker_raw_' + event,
        timeout_seconds=60*60, # Timeout in seconds
        databricks_retry_delay=60, # Time between retries in seconds
        databricks_retry_limit=3, # Retry limit

        notebook_task=generate_notebook_task('/production/jobs/dwh-import/appsflyer_datalocker',
            parameters={
                'START_DATE': '{{  macros.ds_add(ds, -2) }}',
                'END_DATE': '{{  macros.ds_add(ds, +1) }}',
                'EVENT': event,
                'ENVIRONMENT': 'Production',
                'DESTINATION_TABLE_PREFIX': 'RAW_APPSFLYER_DATALOCKER_EVENT.APPSFLYER_DATALOCKER'
            }),

        new_cluster=generate_cluster(
            other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}),

        dag=dag
    )

slice_query = """TO_DATE(_TST) >= '{{ (execution_date - macros.timedelta(days=1)).isoformat() }}'""""" " AND "  """TO_DATE(_TST) < '{{ (execution_date + macros.timedelta(days=1)).isoformat() }}'"""""
sessions_query = ""

sf_rawtol1_datalocker_sessions = SnowflakeOperator(
    task_id="sf_rawtol1_datalocker_sessions",
    sql=prepare_sql_file("dwh-import_appsflyer_event/l1_import.sql",
        params = {
            'TARGET_TABLE': '"KG_DWH"."L1_APPSFLYER_DATALOCKER_EVENT"."APPSFLYER_DATALOCKER_SESSIONS"',
            'SOURCE_TABLE': '"KG_DWH"."RAW_APPSFLYER_DATALOCKER_EVENT"."APPSFLYER_DATALOCKER_SESSIONS"',
            'IS_RETARGETING': 'FALSE',
            'FILTER_QUERY': sessions_query,
            'SLICE': slice_query
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag,
    execution_timeout=timedelta(minutes=30)
)

sf_active_users = SnowflakeOperator(
    task_id="sf_active_users",
    sql=prepare_sql_file("integration_appsflyer_data-locker/appsflyer_active_users.sql"),
    # Path to query file. Wrapped in utillity function
    autocommit=False,  # Autocommit=False enables transactional insertion
    dag=dag
)


db_appsflyer_datalocker_aws_tasks["sessions"] >> db_appsflyer_datalocker_raw_tasks['sessions'] >> sf_rawtol1_datalocker_sessions >> sf_active_users
db_appsflyer_datalocker_aws_tasks["attributed_ad_revenue"] >> db_appsflyer_datalocker_raw_tasks['attributed_ad_revenue']
db_appsflyer_datalocker_aws_tasks["organic_ad_revenue"] >> db_appsflyer_datalocker_raw_tasks['organic_ad_revenue']
