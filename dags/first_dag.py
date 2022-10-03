from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'alex',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='first_dag',
    default_args=default_args,
    description='Learning Dag',
    start_date=datetime(2022, 10, 3, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='echo Hello Wolrd!!'
    )

    task1

    task2 = BashOperator(
        task_id='second_task',
        bash_command='echo Hello Wolrd!! two!!'
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo Hello Wolrd!! three!!'
    )

    task1.set_downstream(task2)
    task1.set_downstream(task3)
