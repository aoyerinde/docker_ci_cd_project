import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 4, 2),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="monitoring_event-latency",
    default_args=default_args,
    schedule_interval='*/20 * * * *',
    catchup = False
)

db_monitoring_appsflyer_event_latency = DatabricksSubmitRunOperator(
    task_id='db_monitoring_appsflyer_event_latency',
    timeout_seconds=60*30,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/monitoring/appsflyer_event-latency'),

    new_cluster=generate_cluster(
        other = {
            'spark_conf': {
                'spark.sql.autoBroadcastJoinThreshold': '-1',
                'spark.databricks.delta.preview.enabled': 'true',
                'spark.network.timeout': '1200',
                'dfs.adls.oauth2.access.token.provider.type': 'ClientCredential',
                'spark.databricks.service.checkSerialization': 'true',
                'spark.sql.broadcastTimeout': '1200'
            }}
        ),

    libraries=[
        {
            "maven": {
                "coordinates": "com.microsoft.azure:applicationinsights-core:2.3.0"
            }
        }
    ],

    dag=dag
)

db_monitoring_client_sdk_event_latency = DatabricksSubmitRunOperator(
    task_id='db_monitoring_client_sdk_event_latency',
    timeout_seconds=60*30,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/monitoring/client-sdk_event-latency'),

    new_cluster=generate_cluster(
        other = {
            'spark_conf': {
                'spark.sql.autoBroadcastJoinThreshold': '-1',
                'spark.databricks.delta.preview.enabled': 'true',
                'spark.network.timeout': '1200',
                'dfs.adls.oauth2.access.token.provider.type': 'ClientCredential',
                'spark.databricks.service.checkSerialization': 'true',
                'spark.sql.broadcastTimeout': '1200'
            }}
        ),

    libraries=[
        {
            "maven": {
                "coordinates": "com.microsoft.azure:applicationinsights-core:2.3.0"
            }
        }
    ],

    dag=dag
)

dummy_final_task = DummyOperator(
    task_id='dummy_final_task',
    trigger_rule='all_done',
    dag=dag,
)

[db_monitoring_appsflyer_event_latency, db_monitoring_client_sdk_event_latency] >> dummy_final_task
