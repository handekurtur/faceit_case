from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from etl import *
from create_insert_sql import *


default_args ={
    'owner': 'faceit',
    'retries': 5, 
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'platform_analytics_pipeline',
    default_args= default_args,
    start_date= datetime(2022, 5, 16),
    schedule_interval= '@daily'
) as dag:

    task1 = BashOperator(
        task_id = 'start',
        bash_command= "echo Start!",
        dag=dag
    )

    task2 = PythonOperator(
        task_id = 'create_target_db_and_tables',
        python_callable= main,
        dag=dag
    )

    task3 = PythonOperator(
        task_id = 'process_and_load_data',
        python_callable= main_etl,
        dag=dag
    )   

    task4 = BashOperator(
        task_id = 'finish',
        bash_command= "echo Done!",
        dag=dag
    )   

    task1 >> task2 >> task3 >> task4
