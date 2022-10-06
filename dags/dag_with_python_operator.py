from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.python import PythonOperator


def my_function(ti):
    name = ti.xcom_pull(task_ids='my_function1', key='name')
    age = ti.xcom_pull(task_ids='my_function1', key='age')
    return f'Hello World {name}, and my age is {age}'


def my_function1(ti):
    # return 'Vishal'
    ti.xcom_push(key='name', value='Vishal')
    ti.xcom_push(key='age', value='21')


default_args = {
    'owner': 'alex',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='python_dag',
    default_args=default_args,
    description='python dag',
    start_date=datetime(2022, 10, 3, 2),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='my_function',
        python_callable=my_function,
        # op_kwargs={'name': 'Vishal'}
    )

    task2 = PythonOperator(
        task_id='my_function1',
        python_callable=my_function1,
    )

    task2 >> task1
    