# DEFINITION OF DEFAULT VALUES
DEFAULT_CLUSTER_NUM_WORKERS = 1
DEFAULT_CLUSTER_NODE_TYPE_ID = 'Standard_DS3_v2'
DEFAULT_CLUSTER_CUSTOM_TAGS = {
    'Department': 'DataEngineering',
    'Environment': 'Production'
}
DEFAULT_CLUSTER_SPARK_VERSION = '5.5.x-scala2.11'
DEFAULT_CLUSTER_PYTHON_VERSION = '/databricks/python3/bin/python3'
DEFAULT_NOTEBOOK_PARAMETERS = {
    'START_DATE' : '{{ ds }}',
    'END_DATE' : '{{ tomorrow_ds }}',
}


def generate_cluster(
      node_type_id = DEFAULT_CLUSTER_NODE_TYPE_ID,
      num_workers = DEFAULT_CLUSTER_NUM_WORKERS,
      other = {}):
    """
    Return a databricks cluster definition

    Args:
      note_type_id (str): (optional, default: 'Standard_DS3_v2') Type of node to spin up, such as Standard_F8s.
      num_workers (int): (optional, default 2) Defines the number of workers
      other (dict): (optional, default {}) Sets and overwrites any cluster configuration present in given dictionary.
        E.g. other = {'spark_conf': {'spark.driver.maxResultSize': '16g'}}
        See: https://docs.microsoft.com/en-gb/azure/databricks/dev-tools/api/latest/jobs#jobsclusterspecnewcluster.

    Returns:
      dict: configuration of new databricks job cluster

    """
    params = {
            'node_type_id': node_type_id,
            'num_workers': num_workers,
            'spark_version': DEFAULT_CLUSTER_SPARK_VERSION,
            'custom_tags': DEFAULT_CLUSTER_CUSTOM_TAGS,
            'spark_env_vars': {
                'PYSPARK_PYTHON': DEFAULT_CLUSTER_PYTHON_VERSION
             }
        }

    for k in other:
        params[k] = other[k]

    return params


def generate_notebook_task(notebook_path, parameters = {}):
    """
    Generate a notebook tasks.
    The parameters START_DATE and END_DATE are automatically appended,
        but can be overwritten via 'parameters'.

    - notebook_path: Path to notebook in Databricks workspace
    - parameters: JSON object to manually append parameters.
        Eg. parameters = {
                'param1': 'Hello World',
                'param2': 42.
                'param3': '{{ task_instance_key_str }}'
            }
    """

    base_parameters = DEFAULT_NOTEBOOK_PARAMETERS

    for k in parameters:
        base_parameters[k] = parameters[k]

    return {
            # Path to notebook to run. Eg. '/production/jobs/integration/business_cost'
            'notebook_path': notebook_path,

            # Arguments send to notebook. Notebook can accept them via dbutils.widgets
            'base_parameters': base_parameters
        }
