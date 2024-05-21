from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

from etl_api import extract_api, transform_api, load_api
from etl_db import extract_db, extract_api_query, transform_db, merge, load_new
from kafka_streaming import stream_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 11),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'proyect_google_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',
) as dag:

    task_read_api = PythonOperator(
        task_id='read_api',
        python_callable=extract_api,
        provide_context=True,
    )

    task_transform_api = PythonOperator(
        task_id='transform_api',
        python_callable=transform_api,
        provide_context=True,
    )

    task_load_api = PythonOperator(
        task_id='load_api',
        python_callable=load_api,
        provide_context=True,
    )

    task_read_db = PythonOperator(
        task_id='read_db',
        python_callable=extract_db,
        provide_context=True,
    )

    task_transform_db = PythonOperator(
        task_id='transform_db',
        python_callable=transform_db,
        provide_context=True,
    )

    task_load_db = PythonOperator(
        task_id='load_db',
        python_callable=load_new,
        provide_context=True,
    )

    task_merge = PythonOperator(
        task_id='merge',
        python_callable=merge,
        provide_context=True,
    )

    task_extract_api_query = PythonOperator(
        task_id='extract_api_query',
        python_callable=extract_api_query,
        provide_context=True,
    )

    task_kafka = PythonOperator(
        task_id='stream_kafka_producer',
        python_callable=stream_data,
        provide_context=True,
    )


    task_read_api >> task_transform_api >> task_load_api
    task_extract_api_query >> task_merge
    task_read_db >> task_merge >> task_transform_db >> task_load_db >> task_kafka
