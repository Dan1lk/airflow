from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 20),
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('dag_variables', default_args=default_args, schedule_interval=None, catchup=False,
          max_active_tasks=3, max_active_runs=1, tags=["variables", "learn variables"])

v1_value = Variable.get('var_1')
v2_value = Variable.get('my_secret_password')
v3_value = Variable.get("json_variable", deserialize_json=True)


task1 = BashOperator(
    task_id='task1',
    bash_command='python3 /airflow/scripts/dag_variables/main_script.py --variable ' + v1_value,
    dag=dag)

task2 = BashOperator(
    task_id='task2',
    bash_command='python3 /airflow/scripts/dag_variables/main_script.py --variable ' + v2_value,
    dag=dag)

for one_value in v3_value.get("list_val"):
    some_task = BashOperator(
        task_id=one_value,
        bash_command='python3 /airflow/scripts/dag_variables/main_script.py --variable ' + one_value,
        dag=dag)
    task2 >> some_task

task1 >> task2