from datetime import timedelta, datetime
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
sys.path.append(os.path.abspath("/opt/airflow/dags/Airflow"))
from Airflow.dags.T_Api import extract_api, transform_api, load_api
from Airflow.dags.T_db import extract_db, transform_db, load_db
from Airflow.dags.merge import merge,load_merge


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 11),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    'proyect_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',
) as dag:

    read_api = PythonOperator(
        task_id = 'read_api',
        python_callable = extract_api,
        provide_context = True,
    )

    transform_api = PythonOperator(
        task_id = 'transform_api',
        python_callable = transform_api,
        provide_context = True,
    )

    load_api = PythonOperator(
        task_id ='load_api',
        python_callable = load_api,
        provide_context = True,
    )

    read_db = PythonOperator(
        task_id = 'read_db',
        python_callable = extract_db,
        provide_context = True,
    )

    transform_db = PythonOperator(
        task_id = 'transform_db',
        python_callable = transform_db,
        provide_context = True,
    )

    load_db = PythonOperator(
        task_id ='load',
        python_callable = load_db,
        provide_context = True,
    )

    merge = PythonOperator(
        task_id ='merge',
        python_callable = merge,
        provide_context = True,
    )


    read_api >> transform_api >> merge >> load_api
    read_db >> transform_db >> merge >> load_db