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
    'start_date': datetime(2020, 10, 30),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1
}

dag = DAG(
    dag_id="integration_google_sheet_store-id-descriptors",
    default_args=default_args,
    schedule_interval = '0 10 * * *', # daily at 10 AM UTC
    catchup = False
)

db_google_sheet_store_id_descriptors = DatabricksSubmitRunOperator(
    task_id='db_google_sheet_store_id_descriptors',
    timeout_seconds=60 * 60 * 1,
    databricks_retry_delay=60,
    databricks_retry_limit=1,

    notebook_task=generate_notebook_task('/production/jobs/integration/google_sheet_shared',
                                         parameters={
                                             'CREDENTIALS_FILE_SECRET_SCOPE': 'google_sheet',
                                             'CREDENTIALS_FILE_SECRET_KEY': 'keyfile',
                                             'GOOGLE_SHEET_NAME': 'Purchase Naming',
                                             'WORKSHEET_TAB_NAME': 'DWH_IMPORT_SHEET',
                                             'COLUMN_ROW_NUMBER': '2',
                                             'DATA_ROW_NUMBER': '3',
                                             'STORAGE_DATASET': 'Store_Id_Descriptors',
                                             'STORAGE_ACCESS_VIEW': 'Access.Store_Id_Descriptors',
                                             'STORAGE_TABLE_NAME': 'RAW_GAMEDATA.STORE_ID_DESCRIPTORS'
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
    ),
    dag=dag
)

# tasks definition for Snowflake RAW to L1
sf_rawtol1_store_id_descriptors= SnowflakeOperator(
    task_id="sf_rawtol1_store_id_descriptors",
    sql=prepare_sql_file("google_sheet/store_id_descriptors.sql",
        params = {
            'TARGET_TABLE': 'L1_GAMEDATA.STORE_ID_DESCRIPTORS',
            'SOURCE_TABLE': 'RAW_GAMEDATA.STORE_ID_DESCRIPTORS',
                    }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)


db_google_sheet_store_id_descriptors >> sf_rawtol1_store_id_descriptors
