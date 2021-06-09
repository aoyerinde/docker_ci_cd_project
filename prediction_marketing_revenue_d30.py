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
    'start_date': datetime(2020, 9, 11),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="prediction_marketing_revenue_d30",
    default_args=default_args,
    schedule_interval = '0 0 * * 5', # once at 0:00 every Friday
    catchup=False
)

DATABASE_ENV = "KG_STAGING" if platform.node() == "vm-airflow-staging" else "KG_DWH"
location = "staging" if platform.node() == "vm-airflow-staging" else "production"

find_coef = DatabricksSubmitRunOperator(
    task_id='find_coef',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    trigger_rule='all_done',

    notebook_task=generate_notebook_task(f'/predictive_analytics/{location}/marketing_revenue_d30/jobs/calculate_coefficients'),
    new_cluster=generate_cluster(other={"num_workers": 1,
                                        "spark_version": "7.3.x-cpu-ml-scala2.12",
                                        "spark_conf": {"spark.databricks.service.server.enabled": "true",
                                                       "spark.databricks.delta.preview.enabled": "true"},
                                        "node_type_id": "Standard_D16s_v3",
                                        "driver_node_type_id": "Standard_D16s_v3",
                                        "spark_env_vars": {"DATABASE_ENV": DATABASE_ENV},
                                        "enable_elastic_disk": True,
                                        "cluster_source": "UI"}),
    dag=dag
)

sanity_check = DatabricksSubmitRunOperator(
    task_id='sanity_check',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    trigger_rule='all_done',

    notebook_task=generate_notebook_task(f'/predictive_analytics/{location}/marketing_revenue_d30/jobs/sanity_checks'),
    new_cluster=generate_cluster(other={"num_workers": 1,
                                        "spark_version": "7.3.x-cpu-ml-scala2.12",
                                        "spark_conf": {"spark.databricks.service.server.enabled": "true",
                                                       "spark.databricks.delta.preview.enabled": "true"},
                                        "node_type_id": "Standard_D16s_v3",
                                        "driver_node_type_id": "Standard_D16s_v3",
                                        "spark_env_vars": {"DATABASE_ENV": DATABASE_ENV},
                                        "enable_elastic_disk": True,
                                        "cluster_source": "UI"}),
    dag=dag
)


find_coef >> sanity_check
