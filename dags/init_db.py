from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator



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

    run_init = BashOperator(
        task_id='run_schemaInit',
        bash_command='python /opt/airflow/executable/exec_init.py ',
    )

run_init
