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
    'start_date': datetime(2020, 4, 1),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="integration_applovin-max-adrev",
    default_args=default_args,
    schedule_interval = '0 3,9,15 * * *', # daily at 3,9,15 UTC,
)

db_integration_applovin_max_adrev = DatabricksSubmitRunOperator(
    task_id='db_integration_applovin_max_adrev',
    timeout_seconds=120 * 60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/applovin_max-revenue',
                                         parameters={
                                             'START_DATE': '{{ macros.ds_add(ds, -5) }}',
                                             'END_DATE': '{{ macros.ds_add(ds, 1) }}',
                                             'DESTINATION_TABLE': 'RAW_ADOPS.APPLOVIN_MAX_AD_REVENUE',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.rpc.message.maxSize': 1024}}
    ),
    dag=dag
)

slice_query_applovin = """DATE >= '{{ macros.ds_add(ds, -5) }}'""""" " AND "  """ DATE < '{{ macros.ds_add(ds, 1) }}'"""""

sf_rawtol1_applovin_adops = SnowflakeOperator(
    task_id="sf_rawtol1_applovin_adops",
    sql=prepare_sql_file("integration_applovin/applovin_max_adops.sql",
        params = {
            'TARGET_TABLE': 'L1_ADOPS.APPLOVIN_MAX_ADOPS',
            'SOURCE_TABLE': 'RAW_ADOPS.APPLOVIN_MAX_AD_REVENUE',
            'SLICE':slice_query_applovin
,
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

sf_rawtol1_applovin_revenue = SnowflakeOperator(
    task_id="sf_rawtol1_applovin_revenue",
    sql=prepare_sql_file("integration_applovin/applovin_max_revenue.sql",
        params = {
            'TARGET_TABLE': 'L1_ADOPS.APPLOVIN_MAX_REVENUE',
            'SOURCE_TABLE': 'RAW_ADOPS.APPLOVIN_MAX_AD_REVENUE',
            'SLICE':slice_query_applovin
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

db_integration_applovin_max_adrev >> [sf_rawtol1_applovin_adops, sf_rawtol1_applovin_revenue] >> dummy_final_task
