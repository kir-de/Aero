from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the Python function that will be run
def run_python_script():
    exec(open("/opt/airflow/executable/exec_init.py").read())

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('schema_init',
         default_args=default_args,
         description='Schema initialize',
         schedule_interval=None,
         catchup=False) as dag:

    run_init = PythonOperator(
        task_id='run_schemaInit',
        python_callable=run_python_script,
    )

run_init
