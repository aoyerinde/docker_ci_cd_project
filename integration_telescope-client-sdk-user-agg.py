import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert

# Note: The connections towards Postgres need to be created in Airflow Connections in advance

default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 10, 30),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1
}

dag = DAG(
    dag_id="integration_telescope-client-sdk-user-agg",
    default_args=default_args,
    schedule_interval = None, # aggregation_clientsdk_user-level dag will trigger this dag
    catchup = False,
    max_active_runs = 1
)

### PROD ###

db_telescope_client_sdk_user_agg_prod = DatabricksSubmitRunOperator(
    task_id='db_client-sdk-user-agg_prod',
    timeout_seconds=60 * 60 * 4,
    databricks_retry_delay=60,
    databricks_retry_limit=1,

    notebook_task=generate_notebook_task('/production/jobs/integration/telescope_client-sdk-user-agg',
                                         parameters={
                                             'DB_COLUMNS': 'playfab_id,bundle_identifier,install_time_first,device_time_changed_to_future,session_latest,organicattribution_first,countrycode_latest,operatingsystem_latest,total_session_count,appversioncurrent,ad_watched_count,purchase_count,total_purchase_revenue_after_revshare_after_vat,highest_unlocked_mine,highest_unlocked_shaft',
                                             'LAST_N_DAYS_SESSION_LATEST': '90', # 0=infinite
                                             'PGL_USER': 'adminprod@pgdb-telescope-prod',
                                             'PGL_PASS_KEY': 'kg-pgdb-telescope-prod.password',
                                             'DESTINATION_SERVER': 'pgdb-telescope-prod',
                                             'DESTINATION_SCHEMA': 'telescope-prod',
                                             'DESTINATION_TABLE': 'clientsdk_aggregated_user_landing',
                                             'REPARTITION_SIZE': '20',
                                             'ALTERNATE_WRITE': 'Auth separately',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
    ),
    dag=dag
)

pg_telescope_update_trigger_prod = PostgresOperator(
    task_id='pg_update_trigger_prod',
    postgres_conn_id='postgres_telescope_prod',
    sql='integration_telescope/trigger_refresh_materialized_view.sql',
    dag=dag
)

pg_telescope_cleanup_table_prod = PostgresOperator(
    task_id='pg_cleanup_table_prod',
    postgres_conn_id='postgres_telescope_prod',
    sql='integration_telescope/trigger_cleanup_table_landing.sql',
    dag=dag
)



### STAGE ###

db_telescope_client_sdk_user_agg_stage = DatabricksSubmitRunOperator(
    task_id='db_client-sdk-user-agg_stage',
    timeout_seconds=60 * 60 * 4,
    databricks_retry_delay=60,
    databricks_retry_limit=1,

    notebook_task=generate_notebook_task('/production/jobs/integration/telescope_client-sdk-user-agg',
                                         parameters={
                                             'DB_COLUMNS': 'playfab_id,bundle_identifier,install_time_first,device_time_changed_to_future,session_latest,organicattribution_first,countrycode_latest,operatingsystem_latest,total_session_count,appversioncurrent,ad_watched_count,purchase_count,total_purchase_revenue_after_revshare_after_vat,highest_unlocked_mine,highest_unlocked_shaft',
                                             'LAST_N_DAYS_SESSION_LATEST': '90', # 0=infinite
                                             'PGL_USER': 'adminprod@pgdb-telescope-prod',
                                             'PGL_PASS_KEY': 'kg-pgdb-telescope-prod.password',
                                             'DESTINATION_SERVER': 'pgdb-telescope-prod',
                                             'DESTINATION_SCHEMA': 'telescope-staging',
                                             'DESTINATION_TABLE': 'clientsdk_aggregated_user_landing',
                                             'REPARTITION_SIZE': '20',
                                             'ALTERNATE_WRITE': 'Auth separately',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
    ),
    dag=dag
)

pg_telescope_update_trigger_stage = PostgresOperator(
    task_id='pg_update_trigger_stage',
    postgres_conn_id='postgres_telescope_stage',
    sql='integration_telescope/trigger_refresh_materialized_view.sql',
    dag=dag
)

pg_telescope_cleanup_table_stage = PostgresOperator(
    task_id='pg_cleanup_table_stage',
    postgres_conn_id='postgres_telescope_stage',
    sql='integration_telescope/trigger_cleanup_table_landing.sql',
    dag=dag
)


### DEV ###

db_telescope_client_sdk_user_agg_dev = DatabricksSubmitRunOperator(
    task_id='db_client-sdk-user-agg_dev',
    timeout_seconds=60 * 60 * 4,
    databricks_retry_delay=60,
    databricks_retry_limit=1,

    notebook_task=generate_notebook_task('/production/jobs/integration/telescope_client-sdk-user-agg',
                                         parameters={
                                             'DB_COLUMNS': 'playfab_id,bundle_identifier,install_time_first,device_time_changed_to_future,session_latest,organicattribution_first,countrycode_latest,operatingsystem_latest,total_session_count,appversioncurrent,ad_watched_count,purchase_count,total_purchase_revenue_after_revshare_after_vat,highest_unlocked_mine,highest_unlocked_shaft',
                                             'LAST_N_DAYS_SESSION_LATEST': '90', # 0=infinite
                                             'PGL_USER': 'admindev@kg-pgdb-telescope-dev',
                                             'PGL_PASS_KEY': 'kg-pgdb-telescope-dev.password',
                                             'DESTINATION_SERVER': 'kg-pgdb-telescope-dev',
                                             'DESTINATION_SCHEMA': 'telescope',
                                             'DESTINATION_TABLE': 'clientsdk_aggregated_user_landing',
                                             'REPARTITION_SIZE': '10',
                                             'ALTERNATE_WRITE': 'Auth separately',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
    ),
    dag=dag
)

pg_telescope_update_trigger_dev = PostgresOperator(
    task_id='pg_update_trigger_dev',
    postgres_conn_id='postgres_telescope_dev',
    sql='integration_telescope/trigger_refresh_materialized_view.sql',
    dag=dag
)

pg_telescope_cleanup_table_dev = PostgresOperator(
    task_id='pg_cleanup_table_dev',
    postgres_conn_id='postgres_telescope_dev',
    sql='integration_telescope/trigger_cleanup_table_landing.sql',
    dag=dag
)

# to apply the schema changes to the view:
# NOTE: any changes to the production view requires sync with Telescope team in advance
#
#pg_telescope_update_trigger_prod = PostgresOperator(
#    task_id='pg_create_trigger_prod',
#    postgres_conn_id='postgres_telescope_prod',
#    sql='integration_telescope/trigger_create_materialized_view.sql',
#    dag=dag
#)
#pg_telescope_update_trigger_stage = PostgresOperator(
#    task_id='pg_create_trigger_stage',
#    postgres_conn_id='postgres_telescope_stage',
#    sql='integration_telescope/trigger_create_materialized_view.sql',
#    dag=dag
#)
#pg_telescope_update_trigger_dev = PostgresOperator(
#    task_id='pg_create_trigger_dev',
#    postgres_conn_id='postgres_telescope_dev',
#    sql='integration_telescope/trigger_create_materialized_view.sql',
#    dag=dag
#)

dummy_final_task = DummyOperator(
    task_id='dummy_final_task',
    trigger_rule='all_done',
    dag=dag
)

# Note: stage and prod are using the same Postgres server. Sequencial task run is preferred instead of running Prod and Stage concurrently.
[db_telescope_client_sdk_user_agg_dev , db_telescope_client_sdk_user_agg_stage] >> db_telescope_client_sdk_user_agg_prod >> [pg_telescope_update_trigger_stage, pg_telescope_update_trigger_dev] >> pg_telescope_update_trigger_prod >> [pg_telescope_cleanup_table_stage, pg_telescope_cleanup_table_dev] >> pg_telescope_cleanup_table_prod >> dummy_final_task
