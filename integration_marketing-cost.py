import airflow
from datetime import timedelta, datetime
from airflow import DAG

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator

from shared_functions.utilities_snowflake import prepare_sql_file
from shared_functions.utilities_databricks import generate_cluster, generate_notebook_task
from shared_functions.utilities_slack import trigger_slack_alert


default_args = {
    'owner': 'Data Platform',
    'start_date': datetime(2020, 3, 26),
    'on_failure_callback': trigger_slack_alert,
    'retries': 1,
    'sla': timedelta(hours=2)
}

dag = DAG(
    dag_id="integration_marketing-cost",
    default_args=default_args,
    schedule_interval = '0 3,4,5,11 * * *', # daily at 3AM, 4AM, 5AM and 11AM UTC
    catchup=False
)


db_asa_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_asa_marketing_cost',
    timeout_seconds=30*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/apple-ads_marketing-cost',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -14) }}',
                                             'END_DATE': '{{  macros.ds_add(ds, -2) }}',
                                             'YESTERDAY': '{{  macros.ds_add(ds, -1) }}',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
    ),
    dag=dag
)

db_crossinstall_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_crossinstall_marketing_cost',
    timeout_seconds=60*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/crossinstall_marketing-cost',
            parameters = {
                'start_days_back': '1',
                'stop_days_back': '1'
            }),
    new_cluster=generate_cluster(
        other={
            'init_scripts' : [
                {
                "dbfs": {
                        "destination":"dbfs:/databricks/scripts/tornado.sh"
                        }
                }
            ]
            }),
    dag=dag
)

db_applovin_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_applovin_marketing_cost',
    timeout_seconds=30*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/applovin_marketing-cost'),
    new_cluster=generate_cluster(),
    dag=dag
)

db_facebook_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_facebook_marketing_cost',
    timeout_seconds=120*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/facebook_marketing-cost'),
    new_cluster=generate_cluster(
        other={
            'init_scripts' : [
                {
                "dbfs": {
                        "destination":"dbfs:/databricks/scripts/tornado.sh"
                        }
                }
            ]
            }),
    dag=dag
)


db_tiktok_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_tiktok_marketing_cost',
    timeout_seconds=30*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/tiktok_marketing-cost'),
    new_cluster=generate_cluster(),
    dag=dag
)


db_kayzen_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_kayzen_marketing_cost',
    timeout_seconds=30*60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/kayzen_marketing-cost'),
    new_cluster=generate_cluster(),
    dag=dag
)


db_snapchat_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_snapchat_marketing_cost',
    timeout_seconds=120 * 60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/snapchat_marketing-cost_v2',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -2) }}',
                                             'END_DATE': '{{  macros.ds_add(ds, +1) }}',
                                             'SOURCE': 'Snapchat',
                                             'DESTINATION_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_SNAPCHAT',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
    ),
    dag=dag
)

slice_query_snapchat = """DATE >= '{{  macros.ds_add(ds, -2) }}'""""" " AND "  """ DATE < '{{  macros.ds_add(ds, +0) }}'"""""

sf_rawtol1_snapchat_marketing_cost = SnowflakeOperator(
    task_id="sf_rawtol1_snapchat_marketing_cost",
    sql=prepare_sql_file("marketing_cost/snapchat.sql",
        params = {
            'TARGET_TABLE': 'L1_MARKETING_COST.MARKETING_COST_SNAPCHAT',
            'SOURCE_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_SNAPCHAT',
            'SLICE':slice_query_snapchat
,
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)


db_taboola_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_taboola_marketing_cost',
    timeout_seconds=120 * 60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/taboola_marketing-cost',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -3) }}',
                                             'END_DATE': '{{  macros.ds_add(ds, +1) }}',
                                             'DESTINATION_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_TABOOLA',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
    ),
    dag=dag
)

slice_query_taboola = """DATE >= '{{  macros.ds_add(ds, -3) }}'""""" " AND "  """ DATE < '{{  macros.ds_add(ds, +0) }}'"""""

sf_rawtol1_taboola_marketing_cost = SnowflakeOperator(
    task_id="sf_rawtol1_taboola_marketing_cost",
    sql=prepare_sql_file("marketing_cost/taboola.sql",
        params = {
            'TARGET_TABLE': 'L1_MARKETING_COST.MARKETING_COST_TABOOLA',
            'SOURCE_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_TABOOLA',
            'SLICE':slice_query_taboola
,
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

db_zucks_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_zucks_marketing_cost',
    timeout_seconds=60 * 60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/zucks_marketing_cost',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -30) }}',
                                             'END_DATE': '{{  macros.ds_add(ds, +1) }}',
                                             'DESTINATION_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_ZUCKS',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
    ),
    dag=dag
)

slice_query_zucks = """DATE >= '{{  macros.ds_add(ds, -30) }}'""""" " AND "  """ DATE < '{{  macros.ds_add(ds, +1) }}'"""""

sf_rawtol1_zucks_marketing_cost = SnowflakeOperator(
    task_id="sf_rawtol1_zucks_marketing_cost",
    sql=prepare_sql_file("marketing_cost/zucks.sql",
        params = {
            'TARGET_TABLE': 'L1_MARKETING_COST.MARKETING_COST_ZUCKS',
            'SOURCE_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_ZUCKS',
            'SLICE':slice_query_zucks
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

db_ironsource_cpe_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_ironsource_cpe_marketing_cost',
    timeout_seconds=60 * 60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/ironsource_cpe-marketing-cost',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -30) }}',
                                             'END_DATE': '{{  macros.ds_add(ds, +0) }}',
                                             'DESTINATION_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_IRONSRC_CPE',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
    ),
    dag=dag
)



slice_query_ironsource_cpe = """DATE >= '{{  macros.ds_add(ds, -30) }}'""""" " AND "  """ DATE < '{{  macros.ds_add(ds, +0) }}'"""""

sf_rawtol1_ironsource_cpe_marketing_cost = SnowflakeOperator(
    task_id="sf_rawtol1_ironsource_cpe_marketing_cost",
    sql=prepare_sql_file("marketing_cost/ironsource_cpe.sql",
        params = {
            'TARGET_TABLE': 'L1_MARKETING_COST.MARKETING_COST_IRONSRC_CPE',
            'SOURCE_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_IRONSRC_CPE',
            'SLICE':slice_query_ironsource_cpe
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)


db_google_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_google_marketing_cost',
    timeout_seconds=60 * 60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/google_marketing-cost',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -7) }}',
                                             'END_DATE': '{{  macros.ds_add(ds, -1) }}',
                                             'YESTERDAY_DATE': '{{  macros.ds_add(ds, +0) }}',
                                             'DESTINATION_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_GOOGLE',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
    ),
    dag=dag
)



slice_query_google = """DATE >= '{{  macros.ds_add(ds, -7) }}'""""" " AND "  """ DATE < '{{  macros.ds_add(ds, +1) }}'"""""

sf_rawtol1_google_marketing_cost = SnowflakeOperator(
    task_id="sf_rawtol1_google_marketing_cost",
    sql=prepare_sql_file("marketing_cost/google.sql",
        params = {
            'TARGET_TABLE': 'L1_MARKETING_COST.MARKETING_COST_GOOGLE',
            'SOURCE_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_GOOGLE',
            'SLICE':slice_query_google
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)


db_appreciate_marketing_cost = DatabricksSubmitRunOperator(
    task_id='db_appreciate_marketing_cost',
    timeout_seconds=60 * 60,
    databricks_retry_delay=60,
    databricks_retry_limit=3,

    notebook_task=generate_notebook_task('/production/jobs/integration/appreciate_marketing-cost',
                                         parameters={
                                             'START_DATE': '{{  macros.ds_add(ds, -7) }}',
                                             'END_DATE': '{{  macros.ds_add(ds, -1) }}',
                                             'YESTERDAY_DATE': '{{  macros.ds_add(ds, +0) }}',
                                             'DESTINATION_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_APPRECIATE',
                                         }
                                         ),
    new_cluster=generate_cluster(
        num_workers=1,
        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
    ),
    dag=dag
)



slice_query_appreciate = """DATE >= '{{  macros.ds_add(ds, -7) }}'""""" " AND "  """ DATE < '{{  macros.ds_add(ds, +1) }}'"""""

sf_rawtol1_appreciate_marketing_cost = SnowflakeOperator(
    task_id="sf_rawtol1_appreciate_marketing_cost",
    sql=prepare_sql_file("marketing_cost/appreciate.sql",
        params = {
            'TARGET_TABLE': 'L1_MARKETING_COST.MARKETING_COST_APPRECIATE',
            'SOURCE_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_APPRECIATE',
            'SLICE':slice_query_appreciate
        }), # Path to query file. Wrapped in utillity function
    autocommit = False, # Autocommit=False enables transactional insertion
    dag = dag
)

#db_dataseat_marketing_cost = DatabricksSubmitRunOperator(
#    task_id='db_dataseat_marketing_cost',
#    timeout_seconds=120 * 60,
#    databricks_retry_delay=60,
#    databricks_retry_limit=3,

#    notebook_task=generate_notebook_task('/production/jobs/integration/dataseat_marketing-cost',
#                                         parameters={
#                                             'START_DATE': '{{ macros.ds_add(ds, -3) }}',
#                                             'END_DATE': '{{ ds }}',
#                                             'DESTINATION_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_DATASEAT',
#                                         }
#                                         ),
#    new_cluster=generate_cluster(
#        num_workers=1,
#        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
#    ),
#    dag=dag
#)

#slice_query_dataseat = """DATE >= '{{  macros.ds_add(ds, -3) }}'""""" " AND "  """ DATE < '{{ ds }}'"""""

#sf_rawtol1_dataseat_marketing_cost = SnowflakeOperator(
#    task_id="sf_rawtol1_dataseat_marketing_cost",
#    sql=prepare_sql_file("marketing_cost/dataseat.sql",
#        params = {
#            'TARGET_TABLE': 'L1_MARKETING_COST.MARKETING_COST_DATASEAT',
#            'SOURCE_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_DATASEAT',
#            'SLICE':slice_query_dataseat
#,
#        }), # Path to query file. Wrapped in utillity function
#    autocommit = False, # Autocommit=False enables transactional insertion
#    dag = dag
#)

dummy_final_task = DummyOperator(
    task_id='dummy_final_task',
    trigger_rule='all_done',
    dag=dag,
)


#db_twitter_marketing_cost = DatabricksSubmitRunOperator(
#    task_id='db_twitter_marketing_cost',
#    timeout_seconds=120 * 60,
#    databricks_retry_delay=60,
#    databricks_retry_limit=3,
#
#    notebook_task=generate_notebook_task('/production/jobs/integration/twitter_marketing_cost',
#                                         parameters={
#                                             'START_DATE': '{{  macros.ds_add(ds, -3) }}',
#                                             'END_DATE': '{{  macros.ds_add(ds, +1) }}',
#                                             'DESTINATION_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_TWITTER',
#                                         }
#                                         ),
#    new_cluster=generate_cluster(
#        num_workers=1,
#        other={'spark_conf': {'spark.sql.autoBroadcastJoinThreshold': '-1'}}
#    ),
#    dag=dag
#)
#
#slice_query_twitter = """DATE >= '{{  macros.ds_add(ds, -3) }}'""""" " AND "  """ DATE < '{{  macros.ds_add(ds, +0) }}'"""""
#
#sf_rawtol1_twitter_marketing_cost = SnowflakeOperator(
#    task_id="sf_rawtol1_twitter_marketing_cost",
#    sql=prepare_sql_file("marketing_cost/twitter.sql",
#        params = {
#            'TARGET_TABLE': 'L1_MARKETING_COST.MARKETING_COST_TWITTER',
#            'SOURCE_TABLE': 'RAW_MARKETING_COST.MARKETING_COST_TWITTER',
#            'SLICE':slice_query_twitter
#,
#        }), # Path to query file. Wrapped in utillity function
#    autocommit = False, # Autocommit=False enables transactional insertion
#    dag = dag
#)





[db_asa_marketing_cost, db_facebook_marketing_cost,
db_applovin_marketing_cost, db_tiktok_marketing_cost,db_kayzen_marketing_cost,db_crossinstall_marketing_cost]  >> dummy_final_task

db_snapchat_marketing_cost >> sf_rawtol1_snapchat_marketing_cost >> dummy_final_task

#db_twitter_marketing_cost >> sf_rawtol1_twitter_marketing_cost >> dummy_final_task

db_taboola_marketing_cost >> sf_rawtol1_taboola_marketing_cost >> dummy_final_task

db_zucks_marketing_cost >> sf_rawtol1_zucks_marketing_cost >> dummy_final_task

db_ironsource_cpe_marketing_cost >> sf_rawtol1_ironsource_cpe_marketing_cost >> dummy_final_task

db_google_marketing_cost >> sf_rawtol1_google_marketing_cost >> dummy_final_task

db_appreciate_marketing_cost >> sf_rawtol1_appreciate_marketing_cost >> dummy_final_task

#db_dataseat_marketing_cost >> sf_rawtol1_dataseat_marketing_cost >> dummy_final_task
