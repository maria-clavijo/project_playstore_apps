from datetime import timedelta, datetime
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
sys.path.append(os.path.abspath("/opt/airflow/dags/Airflow"))
from etl_api import extract_api, transform_api, load_api
from etl_db import extract_db, extract_api_query, transform_db, merge, load_new



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
    'proyect_google_dag',
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
        python_callable = load_new,
        provide_context = True,
    )

    merge = PythonOperator(
        task_id ='merge',
        python_callable = merge,
        provide_context = True,
    )

    extract_api_query = PythonOperator(
        task_id ='extract_api_query',
        python_callable = extract_api_query,
        provide_context = True,
    )

    read_api >> transform_api >> load_api
    extract_api_query >> merge
    read_db >> merge >> transform_db >> load_new