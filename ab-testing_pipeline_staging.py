import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from airflow.operators.bash_operator import BashOperator
from shared_functions.utilities_dbt import dbt
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 5, 20),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="ab-testing_pipeline_staging",
    default_args=default_args,
    schedule_interval = '0 5,10 * * *', # daily at 7am (Berlin time)
    catchup=False
)

ab_tests_users = DatabricksSubmitRunOperator(
    task_id='ab_tests_users',
    timeout_seconds=90*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/ab_test_users'),
    new_cluster=generate_cluster(),
    dag=dag
)

config_table = DatabricksSubmitRunOperator(
    task_id='config_table',
    timeout_seconds=30*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/config_table'),
    new_cluster=generate_cluster(),
    dag=dag
)

ab_tests_overview = DatabricksSubmitRunOperator(
    task_id='ab_tests_overview',
    timeout_seconds=30*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/ab_tests_overview'),
    new_cluster=generate_cluster(),
    dag=dag
)

ab_test = DatabricksSubmitRunOperator(
    task_id='ab_test',
    timeout_seconds=30*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/ab_test'),
    new_cluster=generate_cluster(),
    dag=dag
)


health_check_aa_test_machine_learning = DatabricksSubmitRunOperator(
    task_id='health_check_aa_test_machine_learning',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/health_check_aa_test_machine_learning'),
    new_cluster=generate_cluster(),
    dag=dag
)

health_check_multiple_abtest_concurrently = DatabricksSubmitRunOperator(
    task_id='health_check_multiple_abtest_concurrently',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/health_check_multiple_abtest_concurrently'),
    new_cluster=generate_cluster(),
    dag=dag
)

health_check_sufficient_sample_size = DatabricksSubmitRunOperator(
    task_id='health_check_sufficient_sample_size',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/health_check_sufficient_sample_size'),
    new_cluster=generate_cluster(),
    dag=dag
)

health_check_multiple_groups_in_same_test = DatabricksSubmitRunOperator(
  task_id='health_check_multiple_groups_in_same_test',
  timeout_seconds=60*60,
  databricks_retry_delay=60,
  databricks_retry_limit=3,
  notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/health_check_multiple_groups_in_same_ab_test'),
  new_cluster=generate_cluster(),
  dag=dag
)

health_check_equal_user_allocation = DatabricksSubmitRunOperator(
    task_id='health_check_equal_user_allocation',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/health_check_equal_user_allocation_into_group'),
    new_cluster=generate_cluster(),
    dag=dag
)

bootstrapping_for_new_users = DatabricksSubmitRunOperator(
    task_id='bootstrapping_for_new_users',
    timeout_seconds=240*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/bootstrapping_for_new_users'),
    new_cluster=generate_cluster(),
    dag=dag
)

bootstrapping_for_existing_users = DatabricksSubmitRunOperator(
    task_id='bootstrapping_for_existing_users',
    timeout_seconds=240*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,
    notebook_task=generate_notebook_task('/feature-branches/staging_ab-testing/jobs/ab-testing/bootstrapping_for_existing_users'),
    new_cluster=generate_cluster(),
    dag=dag
)

dbt_ab_testing_pipeline = BashOperator(
    task_id='dbt_ab_testing_pipeline',
    bash_command=dbt('run', 'ab-testing'),
    dag=dag
)

dbt_ab_testing_pipeline_test = BashOperator(
    task_id='dbt_ab_testing_pipeline_test',
    bash_command=dbt('test', 'ab-testing'),
    dag=dag
)

config_table >> ab_tests_users >> dbt_ab_testing_pipeline >> dbt_ab_testing_pipeline_test >> [health_check_aa_test_machine_learning, health_check_multiple_abtest_concurrently, health_check_multiple_groups_in_same_test, health_check_equal_user_allocation, bootstrapping_for_new_users, bootstrapping_for_existing_users] >> ab_test >> ab_tests_overview >> health_check_sufficient_sample_size
