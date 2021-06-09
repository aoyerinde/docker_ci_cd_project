import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

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
    dag_id="integration_google-sheet-notebooks",
    default_args=default_args,
    schedule_interval = '0 3,5 * * *', # daily at 3AM, 5AM  UTC,
    catchup =False
)

db_integration_business_cost = DatabricksSubmitRunOperator(
    task_id='db_integration_business_cost',
    timeout_seconds=30*60, 
    databricks_retry_delay=60, 
    databricks_retry_limit=3, 
    
    notebook_task=generate_notebook_task('/production/jobs/integration/business_cost'),
    new_cluster=generate_cluster(),
    dag=dag
)

db_integration_neutral_revenue = DatabricksSubmitRunOperator(
    task_id='db_integration_neutral_revenue',
    timeout_seconds=90*60, 
    databricks_retry_delay=60, 
    databricks_retry_limit=3, 
    
    notebook_task=generate_notebook_task('/production/jobs/integration/business_neutral-revenue'),
    new_cluster=generate_cluster(num_workers = 2),
    dag=dag
)

sf_rawtol1_business_neutralrevenue = SnowflakeOperator(
    task_id="sf_rawtol1_business_neutral_revenue",
    sql=prepare_sql_file("business_confidential/neutral_revenue.sql"),
    # Path to query file. Wrapped in utillity function
    autocommit=False,  # Autocommit=False enables transactional insertion
    dag=dag
)

db_integration_liveops_imt = DatabricksSubmitRunOperator(
    task_id='db_integration_liveops_imt',
    timeout_seconds=30*60, 
    databricks_retry_delay=60, 
    databricks_retry_limit=3, 
    
    notebook_task=generate_notebook_task('/production/jobs/integration/live-ops-imt_event-mine_information'),
    new_cluster=generate_cluster(),
    dag=dag
)


dummy_final_task = DummyOperator(
    task_id='dummy_final_task',
    trigger_rule='all_done',
    dag=dag
)

[db_integration_business_cost, db_integration_neutral_revenue,
db_integration_liveops_imt] >> dummy_final_task

db_integration_neutral_revenue >> sf_rawtol1_business_neutralrevenue >> dummy_final_task

