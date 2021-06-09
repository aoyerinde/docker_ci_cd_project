import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator

from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 4, 1),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="integration_vat-rate",
    default_args=default_args,
    schedule_interval='0 3 * * *',
)

db_integration_vat_rate = DatabricksSubmitRunOperator(
    task_id='db_integration_vat_rate',
    timeout_seconds=60*30,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    new_cluster=generate_cluster(),

    libraries= [
        {
            'jar': 'dbfs:/libraries/job_jar/data_vatrate_0_5_0_jar_with_dependencies.jar'
        }
    ],

    spark_jar_task= {
        'main_class_name': 'com.fluffyfairygames.data.vatrate.VatRate'
    },

    dag=dag
)
