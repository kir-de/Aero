from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import sys

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('nhl_stats',
         default_args=default_args,
         description='NHL data parser',
         schedule_interval=timedelta(hours=12),
         catchup=False) as dag:

    run_parser_task = BashOperator(
        task_id='run_NHLparser',
          bash_command='python /opt/airflow/executable/exec_parser.py ',
    )

run_parser_task
