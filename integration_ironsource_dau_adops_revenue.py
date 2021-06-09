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
    'start_date': datetime(2020, 4, 1),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="integration_ironsource_dau_adops_revenue",
    default_args=default_args,
    schedule_interval='0 2,4,9 * * *',
    catchup = False
)

db_integration_ironsource_dau_adops_revenue = DatabricksSubmitRunOperator(
    task_id='db_integration_ironsource_dau_adops_revenue',
    timeout_seconds=60*90,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/ironsource_dau-adops-revenue',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -5) }}',
                                             'END_DATE': '{{  macros.ds_add(ds, +0) }}',
                                             'SLICE_END_DATE': '{{  macros.ds_add(ds, +1) }}',
                                             'DESTINATION_TABLE': 'RAW_ADOPS.IRONSOURCE_ADOPS',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
    ),
    dag=dag
)


slice_query_ironsource_dau_adops = """_TST >= '{{  macros.ds_add(ds, -5) }}'""""" " AND "  """_TST < '{{  macros.ds_add(ds, +1) }}'"""""
l1_slice_query_ironsource_dau_adops = """DATE >= '{{  macros.ds_add(ds, -5) }}'""""" " AND "  """DATE < '{{  macros.ds_add(ds, +1) }}'"""""

sf_rawtol1_ironsource_dau_adops = SnowflakeOperator(
    task_id="sf_rawtol1_ironsource_dau_adops",
    sql=prepare_sql_file("adops_dau_revenue/ironsource_dau_adops.sql",
        params = {
            'TARGET_TABLE': 'KG_DWH.L1_ADOPS.IRONSOURCE_ADOPS',
            'SOURCE_TABLE': 'RAW_ADOPS.IRONSOURCE_ADOPS',
            'SLICE': slice_query_ironsource_dau_adops,
            'L1_SLICE': l1_slice_query_ironsource_dau_adops
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

slice_query_ironsource_dau_revenue = """DATE >= '{{  macros.ds_add(ds, -7) }}'""""" " AND "  """ DATE < '{{  macros.ds_add(ds, +1) }}'"""""

sf_l1tol2_ironsource_dau_revenue = SnowflakeOperator(
    task_id="sf_l1tol2_ironsource_dau_revenue",
    sql=prepare_sql_file("adops_dau_revenue/ironsource_dau_revenue.sql",
        params = {
            'TARGET_TABLE': 'KG_DWH.L1_ADOPS.IRONSOURCE_DAU_REVENUE',
            'SOURCE_TABLE': 'KG_DWH.L1_ADOPS.IRONSOURCE_ADOPS',
            'SLICE': slice_query_ironsource_dau_revenue
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

db_integration_ironsource_dau_adops_revenue >> sf_rawtol1_ironsource_dau_adops >> sf_l1tol2_ironsource_dau_revenue
