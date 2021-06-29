DBT_PYTHON_ENV = '/home/airflow/dbt-env'
DBT_LOCAL_REPO_PATH = '/home/airflow/dbt-backup/'
DBT_REPO_NAME = 'dp_project'

def dbt(cmd, modules=None):
    bash_command =  'source {dbt_python_env}/bin/activate ;'.format(dbt_python_env=DBT_PYTHON_ENV)
    bash_command += 'cp -r {dbt_local_repo_path} {dbt_repo_name};'\
      .format(dbt_local_repo_path=DBT_LOCAL_REPO_PATH, dbt_repo_name=DBT_REPO_NAME)
    bash_command += 'cd {dbt_repo_name} ;'.format(dbt_repo_name=DBT_REPO_NAME)

    arg_modules = ''
    if modules is not None:
        arg_modules = ' -m {}'.format(modules)

    bash_command += 'dbt deps;'
    bash_command += 'dbt ' + cmd + arg_modules + ';'

    return bash_command
