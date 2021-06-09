import airflow
import platform
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert

default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 12, 29),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="prediction_imt_payer_d90",
    default_args=default_args,
    schedule_interval = '0 0 * * *', # once at 0:00
    catchup=False
)

DATABASE_ENV = "KG_STAGING" if platform.node() == "vm-airflow-staging" else "KG_DWH"
location = "staging" if platform.node() == "vm-airflow-staging" else "production"

configs = {"autoscale": {"min_workers": 1, "max_workers": 3},
           "spark_version": "7.3.x-cpu-ml-scala2.12",
           "spark_conf": {"spark.databricks.service.server.enabled": "true",
                          "spark.databricks.delta.preview.enabled": "true"},
            "node_type_id": "Standard_D32s_v3",
            "driver_node_type_id": "Standard_D32s_v3",
            "spark_env_vars": {"DATABASE_ENV": DATABASE_ENV},
            "enable_elastic_disk": True,
            "cluster_source": "UI"}

inference_d1 = DatabricksSubmitRunOperator(
    task_id='inference_d1',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    trigger_rule='all_done',

    notebook_task=generate_notebook_task(f'/predictive_analytics/{location}/imt_payer_d90/jobs/inference_d1'),
    new_cluster=generate_cluster(other=configs),
    dag=dag
)

inference_d7 = DatabricksSubmitRunOperator(
    task_id='inference_d7',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    trigger_rule='all_done',

    notebook_task=generate_notebook_task(f'/predictive_analytics/{location}/imt_payer_d90/jobs/inference_d7'),
    new_cluster=generate_cluster(other=configs),
    dag=dag
)

actuals = DatabricksSubmitRunOperator(
    task_id='actuals',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    trigger_rule='all_done',

    notebook_task=generate_notebook_task(f'/predictive_analytics/{location}/imt_payer_d90/jobs/actuals'),
    new_cluster=generate_cluster(other=configs),
    dag=dag
)

monitoring = DatabricksSubmitRunOperator(
    task_id='monitoring',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    trigger_rule='all_done',

    notebook_task=generate_notebook_task(f'/predictive_analytics/{location}/imt_payer_d90/jobs/monitoring'),
    new_cluster=generate_cluster(other=configs),
    dag=dag
)

sanity_check = DatabricksSubmitRunOperator(
    task_id='sanity_check',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    trigger_rule='all_done',

    notebook_task=generate_notebook_task(f'/predictive_analytics/{location}/imt_payer_d90/jobs/sanity_checks'),
    new_cluster=generate_cluster(other=configs),
    dag=dag
)

inference_d1 >> inference_d7 >> actuals >> monitoring >> sanity_check
