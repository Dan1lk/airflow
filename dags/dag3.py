from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook
from plagins.dag3_plagin.dag3_operator import ExampleOperator

connection = BaseHook.get_connection('main_postgresql_connection')

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 13),
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('dag3', default_args=default_args, schedule_interval=None, catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["Test"])

task1 = ExampleOperator(
    task_id='task1',
    postgre_conn=connection,
    text_field='Вставка из таски 1',
    dag=dag
)

task2 = ExampleOperator(
    task_id='task2',
    postgre_conn=connection,
    text_field='Вставка из таски 2',
    dag=dag
)


task1 >> task2