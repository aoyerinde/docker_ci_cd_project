import airflow
from datetime import datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert
from shared_functions.utilities_dbt import dbt


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 3, 27),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="integration_appsflyer-masterapi-kpi-jobs",
    default_args=default_args,
    schedule_interval = '0 3,5,6 * * *', # daily at 3AM, 5AM and 6AM UTC,
)

db_af_ad_performance = DatabricksSubmitRunOperator(
    task_id='db_af_ad_performance',
    timeout_seconds=30*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/appsflyer_ad-performance',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -5) }}',
                                             'END_DATE': '{{   macros.ds_add(ds, -2) }}',
                                             'DESTINATION_TABLE': 'RAW_GAMEDATA.APPSFLYER_ADPERFORMANCE',
                                         }),
    new_cluster=generate_cluster(),
    dag=dag
)


dbt_af_l1_ad_performance = BashOperator(
    task_id='dbt_af_l1_ad_performance',
    bash_command=dbt('run', 'tag:appsflyer_ad_performance'),
    dag=dag
)

db_af_dau_metrics = DatabricksSubmitRunOperator(
    task_id='db_af_dau_metrics',
    timeout_seconds=30*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/appsflyer_dau_metrics',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -5) }}',
                                             'END_DATE': '{{  ds }}',
                                             'DESTINATION_TABLE': 'RAW_MARKETING.APPSFLYER_MARKETINGKPI_DAU',
                                         }
                                         ),
    new_cluster=generate_cluster(),
    dag=dag
)

db_af_retention = DatabricksSubmitRunOperator(
    task_id='db_af_retention',
    timeout_seconds=30*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/appsflyer_retention'),
    new_cluster=generate_cluster(),
    dag=dag
)

db_af_explore_kpis = DatabricksSubmitRunOperator(
    task_id='db_af_explore_kpis',
    timeout_seconds=120 * 60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/appsflyer_explore_kpis',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -3) }}',
                                             'END_DATE': '{{  ds }}',
                                             'DESTINATION_TABLE': 'L1_MARKETING.APPSFLYER_CREATIVEKPI',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
    ),
    dag=dag
)

dummy_final_task = DummyOperator(
    task_id='dummy_final_task',
    trigger_rule='all_done',
    dag=dag
)

[db_af_ad_performance, db_af_dau_metrics, db_af_retention, db_af_explore_kpis] >> dbt_af_l1_ad_performance >> dummy_final_task
