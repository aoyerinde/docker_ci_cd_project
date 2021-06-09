
from airflow.models import DAG, DagModel
from airflow.operators.python_operator import PythonOperator
from airflow import settings
from datetime import timedelta, datetime
import os
import os.path
import socket
import logging
import airflow
from shared_functions.utilities_slack import trigger_slack_alert


"""
A maintenance workflow that you can deploy into Airflow to periodically clean out entries in the DAG table of which there is no longer a corresponding Python File for it. This ensures that the DAG table doesn't have needless items in it and that the Airflow Web Server displays only those available DAGs.
airflow trigger_dag airflow-clear-missing-dags
"""


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
ENABLE_DELETE = True

default_args = {
    'owner': 'Data Platform',
    'depends_on_past': False,
    'on_failure_callback': trigger_slack_alert,
    'start_date': datetime(2021, 3, 22),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval= '0 18 * * MON,THU',
    start_date=datetime(2021, 3, 22),
    catchup=False
)

def clear_missing_dags_fn(**context):

    logging.info("Starting to run Clear Process")

    try:
        host_name = socket.gethostname()
        host_ip = socket.gethostbyname(host_name)
        logging.info("Running on Machine with Host Name: " + host_name)
        logging.info("Running on Machine with IP: " + host_ip)
    except Exception as e:
        print("Unable to get Host Name and IP: " + str(e))

    session = settings.Session()

    logging.info("Configurations:")
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("session:                  " + str(session))
    logging.info("")

    dags = session.query(DagModel).all()
    entries_to_delete = []
    for dag in dags:
        # Check if it is a zip-file
        if dag.fileloc is not None and '.zip/' in dag.fileloc:
            index = dag.fileloc.rfind('.zip/') + len('.zip')
            fileloc = dag.fileloc[0:index]
        else:
            fileloc = dag.fileloc

        if fileloc is None:
            logging.info(
                "After checking DAG '" + str(dag) +
                "', the fileloc was set to None so assuming the Python " +
                "definition file DOES NOT exist"
            )
            entries_to_delete.append(dag)
        elif not os.path.exists(fileloc):
            logging.info(
                "After checking DAG '" + str(dag) +
                "', the Python definition file DOES NOT exist: " + fileloc
            )
            entries_to_delete.append(dag)
        else:
            logging.info(
                "After checking DAG '" + str(dag) +
                "', the Python definition file does exist: " + fileloc
            )

    logging.info("Process will be Deleting the DAG(s) from the DB:")
    for entry in entries_to_delete:
        logging.info("\tEntry: " + str(entry))
    logging.info(
        "Process will be Deleting " + str(len(entries_to_delete)) + " DAG(s)"
    )

    if ENABLE_DELETE:
        logging.info("Performing Delete...")
        for entry in entries_to_delete:
            session.delete(entry)
        session.commit()
        logging.info("Finished Performing Delete")
    else:
        logging.warn("You're opted to skip deleting the DAG entries!!!")

    logging.info("Finished Running Clear Process")


clear_missing_dags = PythonOperator(
    task_id='clear_missing_dags',
    python_callable=clear_missing_dags_fn,
    provide_context=True,
    dag=dag)
