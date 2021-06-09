import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from shared_functions.utilities_dbt import dbt
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.dummy_operator import DummyOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 8, 10),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1
}

with DAG(
    dag_id="dwh_import-marketing_creative_performance",
    default_args=default_args,
    schedule_interval = '0 4,5 * * *',
    catchup=False
    ) as dag:

    db_marketing_creative_performance = DatabricksSubmitRunOperator(
        task_id='db_marketing_creative_performance',
        timeout_seconds=60*60,
        databricks_retry_delay=60,
        databricks_retry_limit=1,

        notebook_task=generate_notebook_task('/production/jobs/integration/marketing_creative-information'),
        new_cluster=generate_cluster()
    )

    sf_rawtol1_marketing_creative_performance = SnowflakeOperator(
        task_id="sf_rawtol1_marketing_creative_performance",
        sql=prepare_sql_file("marketing_creative_performance/marketing_creative_performance_information.sql",
            params={
            'TARGET_TABLE': 'L1_MARKETING_CREATIVE_PERFORMANCE.CREATIVE_INFORMATION',
            'SOURCE_TABLE': 'RAW_MARKETING_CREATIVE_PERFORMANCE.CREATIVE_INFORMATION'
            }),
        autocommit = False
    )

db_marketing_creative_performance >> sf_rawtol1_marketing_creative_performance
