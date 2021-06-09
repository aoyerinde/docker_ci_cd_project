import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from shared_functions.utilities_dbt import dbt
from airflow.operators.dummy_operator import DummyOperator
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 8, 23),
    'on_failure_callback': trigger_slack_alert,
    'retries': 0,
    'sla': timedelta(hours=1)
}

dag = DAG(
    dag_id="appsflyer_based_business_model",
    default_args=default_args,
    schedule_interval = '0 4,5,6,7,8,10 * * *'
)

dbt_appsflyer_based_business_model = BashOperator(
    task_id='appsflyer_based_business_model',
    bash_command=dbt('run', 'business.appsflyer_based_business_model'),
    dag=dag
)

dbt_appsflyer_based_business_model_test = BashOperator(
    task_id='dbt_test',
    bash_command=dbt('test', 'business.appsflyer_based_business_model'),
    dag=dag
)


dummy_final_task = DummyOperator(
    task_id='dummy_final_task',
    trigger_rule='all_done',
    dag=dag,
)

dbt_appsflyer_based_business_model >> dbt_appsflyer_based_business_model_test >> dummy_final_task
