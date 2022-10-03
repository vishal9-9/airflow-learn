from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'owner': 'alex'
}


@dag(dag_id='decorator_dag',
     default_args=default_args,
     start_date=datetime(2022, 10, 3, 2),
     schedule_interval='@daily')
def my_test_dag():

    @task()
    def get_name():
        return 'Alex'

    @task()
    def get_age():
        return 21

    @task()
    def get_skill(multiple_outputs=True):
        return {
            'skill': 'python',
            'skill1': 'reactjs'
        }

    @task()
    def get_info(name, age, skill):
        return f'Name id {name}, and Age is {age} with skill {skill["skill"]}, {skill["skill1"]}'

    name = get_name()
    age = get_age()
    skills = get_skill()
    print(skills)
    get_info(name=name, age=age,
             skill=skills)


test_dag = my_test_dag()
