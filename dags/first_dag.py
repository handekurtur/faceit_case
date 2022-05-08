""" from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.check_operator import CheckOperator

default_args ={
    'owner': 'faceit',
    'retries': 5, 
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'first_dag_v4',
    default_args= default_args,
    description= 'this is first',
    start_date= datetime(2022, 5, 8),
    #start_dag = datetime(2022, 5, 8, 2),
    schedule_interval= '@daily'

) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command= "echo hello world task-1 !"
    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command= "echo hello world task -2!"
    )

    task3 = BashOperator(
        task_id = 'third_task',
        bash_command= "echo hello world task -3!"
    )

    #task1.set_downstream(task2)
    #task1.set_downstream(task3)

    #task1 >> task2
    #task1 >> task3

    task1 >> [task2,task3] """