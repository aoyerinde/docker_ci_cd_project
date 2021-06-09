import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2021, 3, 31),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
}

dag = DAG(
    dag_id="dwh-import_imt_game_balance",
    default_args=default_args,
    schedule_interval='0 6 * * MON',
    catchup = False,
    max_active_runs = 1

)

db_artifacts_raw = DatabricksSubmitRunOperator(
    task_id = 'db_artifacts_raw',
    timeout_seconds = 50*60,
    databricks_retry_delay = 60,
    databricks_retry_limit = 3,

    notebook_task = generate_notebook_task(
        '/production/jobs/integration/imt-game_balance',
        parameters = {
            'WORKBOOK': 'Artifacts',
            'WORKSHEET': '_Artifacts'
        }),
    new_cluster = generate_cluster(
        num_workers = 6,
        node_type_id = 'Standard_F4s',
        other = {'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
        ),
    dag = dag
)

db_iapanditems_raw = DatabricksSubmitRunOperator(
    task_id = 'db_iapanditems_raw',
    timeout_seconds = 50*60,
    databricks_retry_delay = 60,
    databricks_retry_limit = 3,

    notebook_task = generate_notebook_task(
        '/production/jobs/integration/imt-game_balance',
        parameters = {
            'WORKBOOK': 'IapsAndItems',
            'WORKSHEET': '_Items'
        }),
    new_cluster = generate_cluster(
        num_workers = 6,
        node_type_id = 'Standard_F4s',
        other = {'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
        ),
    dag = dag
)



db_supermanagers_raw = DatabricksSubmitRunOperator(
    task_id = 'db_supermanagers_raw',
    timeout_seconds = 50*60,
    databricks_retry_delay = 60,
    databricks_retry_limit = 3,

    notebook_task = generate_notebook_task(
        '/production/jobs/integration/imt-game_balance',
        parameters = {
            'WORKBOOK': 'SuperManagers',
            'WORKSHEET': '_SuperManagers'
        }),
    new_cluster = generate_cluster(
        num_workers = 6,
        node_type_id = 'Standard_F4s',
        other = {'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
        ),
    dag = dag
)

db_mineidentifier_raw = DatabricksSubmitRunOperator(
    task_id = 'db_mineidentifier_raw',
    timeout_seconds = 50*60,
    databricks_retry_delay = 60,
    databricks_retry_limit = 3,

    notebook_task = generate_notebook_task(
        '/production/jobs/integration/imt-game_balance',
        parameters = {
            'WORKBOOK': 'MineIdentifier',
            'WORKSHEET': '_MineIdentifier'
        }),
    new_cluster = generate_cluster(
        num_workers = 6,
        node_type_id = 'Standard_F4s',
        other = {'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
        ),
    dag = dag
)


db_dailyreward_raw = DatabricksSubmitRunOperator(
    task_id = 'db_dailyreward_raw',
    timeout_seconds = 50*60,
    databricks_retry_delay = 60,
    databricks_retry_limit = 3,

    notebook_task = generate_notebook_task(
        '/production/jobs/integration/imt-game_balance',
        parameters = {
            'WORKBOOK': 'DailyReward',
            'WORKSHEET': '_DailyReward'
        }),
    new_cluster = generate_cluster(
        num_workers = 6,
        node_type_id = 'Standard_F4s',
        other = {'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
        ),
    dag = dag
)

db_artifacts_raw
db_iapanditems_raw
db_mineidentifier_raw
db_supermanagers_raw
db_dailyreward_raw

