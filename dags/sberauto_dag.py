"""
Модуль sberauto_dag.py - это Airflow DAG, который описывает порядок выполнения задач.

В этом модуле мы создаем DAG, который будет запускаться каждый день в 12:00.
В DAG мы добавили 2 задачи:
- insert_data: выполнит функцию insert_data из модуля data_pipeline, которая загрузит данные
    из файлов json_data в таблицы hits и sessions.
- process_and_load_json_data: выполнит функцию process_and_load_json_data из модуля json_to_db,
    которая обработает данные из файлов json_data и загрузит их в базу данных.

В параметрах DAG мы указали владельца, дату начала выполнения, количество повторов,
    и время ожидания между повторами.

"""
import os
import sys
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator


# Укажем путь к проекту
project_path = os.path.expanduser('~/Projects/diploma')

# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = project_path

# Добавим путь к проекту в системные переменные
sys.path.insert(0, project_path)

from modules.data_pipeline import insert_data
from modules.json_to_db import process_and_load_json_data

default_args = {
    'owner': 'temeka',
    'depends_on_past': False,
    'start_date': dt.datetime(2024, 10, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
        dag_id='sberauto_dag',
        schedule='0 12 * * *',
        default_args=default_args,
        max_active_runs=1,

) as dag:
    task_process_data = PythonOperator(
        task_id='process_data',
        python_callable=insert_data,
        dag=dag,
        task_concurrency=1
    )

    task_process_json = PythonOperator(
        task_id='process_json',
        python_callable=process_and_load_json_data,
        dag=dag,
        task_concurrency=1
    )

    task_process_data >> task_process_json
