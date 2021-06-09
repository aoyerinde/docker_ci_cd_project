import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert

default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 4, 9),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="integration_sensor-tower",
    default_args=default_args,
    schedule_interval = '0 5 * * *', # daily 5am UTC (7am Berlin time)
    catchup=False
)

db_sensortower_store_features = DatabricksSubmitRunOperator(
    task_id='db_sensortower_store_features',
    timeout_seconds=120 * 60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/sensor-tower_store-features',
                                         parameters={
                                             'START_DATE': '{{ macros.ds_add(ds, -2) }}',
                                             'END_DATE': '{{ macros.ds_add(ds, 1) }}',
                                             'DESTINATION_TABLE': 'RAW_MARKETING_CREATIVE_PERFORMANCE.SENSORTOWER_STORE_FEATURES',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
    ),
    dag=dag
)

slice_query_sensortower = """DATE >= '{{  macros.ds_add(ds, -2) }}'""""" " AND "  """ DATE < '{{ macros.ds_add(ds, 1) }}'"""""

sf_rawtol1_sensortower_store_features = SnowflakeOperator(
    task_id="sf_rawtol1_sensortower_store_features",
    sql=prepare_sql_file("marketing_creative_performance/sensortower.sql",
        params = {
            'TARGET_TABLE': 'L1_MARKETING.SENSORTOWER_STORE_FEATURES',
            'SOURCE_TABLE': 'RAW_MARKETING_CREATIVE_PERFORMANCE.SENSORTOWER_STORE_FEATURES',
            'SLICE':slice_query_sensortower
,
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

dummy_final_task = DummyOperator(
    task_id='dummy_final_task',
    trigger_rule='all_done',
    dag=dag,
)

db_sensortower_store_features >> sf_rawtol1_sensortower_store_features >> dummy_final_task
