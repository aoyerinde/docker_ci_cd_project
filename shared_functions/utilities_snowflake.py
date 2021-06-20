import os

# Path to DAGs
HOME_PATH  = "{}/airflow/dags/".format(os.path.expanduser('~'))

def prepare_sql_file(file_path, params = {}):
    """
    Returns the individual quries of a .sql file as a string list

    - file_path: Relative path to the sql file. E.g. example_dag/example_one.sql
    - (optional) params: JSON object that provides custom parameters to the sql file.
                    E.g. { 'param1': 'Hello World', 'param2': 3.141 }
                    Callable via '%(name)s' in .sql file.
                    E.g. "CREATE TABLE %(TABLE_NAME)s (value VARCHAR);"

    """
    path = HOME_PATH + file_path
    with open(path) as f:
        s = f.read()
        s = s.strip("\n")

    s = s.format(**params)

    queries = s.split(";") # split file by semicolons
    queries = [q for q in queries if q != ""] # remove empty strings from list
    return queries
