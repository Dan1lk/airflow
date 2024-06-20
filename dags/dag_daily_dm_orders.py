from datetime import datetime
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 18),
    #"retry_delay": timedelta(minutes=0.1)
}

dag = DAG('dag_daily_dm_orders', default_args=default_args, schedule_interval='0 * * * *', catchup=False,
          max_active_tasks=3, max_active_runs=1, tags=["data marts", "dm_orders"])

clear_day = PostgresOperator(
    task_id='clear_day',
    postgres_conn_id='main_postgresql_connection',
    sql="""DELETE FROM public.dm_orders WHERE "date_order" = '{{ ds }}'::date""",
    dag=dag)

calc_day = PostgresOperator(
    task_id='calc_day',
    postgres_conn_id='main_postgresql_connection',
    sql="""INSERT INTO dm_orders(name_postavshik, name_klient, количество_заказанных_авто, сумма_заказа, date_order)
SELECT name_postavshik, name_klient, count(fo.d_assortiment_id) as Количество_заказанных_авто, sum(da.stoimost::int) as Сумма_заказа, now() as date_order
FROM f_orders fo 
	INNER JOIN d_assortiment da ON fo.d_assortiment_id = da.d_assortiment_id 

	INNER JOIN dostavka d ON fo.dostavka_id = d.dostavks_id 
	INNER JOIN d_postavshik dp ON d.dostavks_id = dp.d_postavshik_id 
	INNER JOIN klients k ON fo.klients_id = k.klients_id 
WHERE "date_order" = '{{ ds }}'::date
GROUP BY name_postavshik, name_klient""",
    dag=dag)

clear_day >> calc_day