from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import sys
import os

sys.path.insert(0, '/opt/airflow/src')
from extract_flights import get_schiphol_flights, upload_to_minio
from extract_weather import get_schiphol_weather
from load_to_postgres import load_json_to_postgres

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

MINIO_CONN_ID = "minio_conn"
DWH_CONN_ID = "dwh_conn"

def extract_flights_task(**context):
    target_date = context['data_interval_start'].strftime("%Y-%m-%d")
    logging.info(f"Initiating flight extraction for {target_date}")
    
    app_id = Variable.get("SCHIPHOL_APP_ID", default_var=os.environ.get("SCHIPHOL_APP_ID"))
    app_key = Variable.get("SCHIPHOL_APP_KEY", default_var=os.environ.get("SCHIPHOL_APP_KEY"))
    
    flights_data = get_schiphol_flights(app_id, app_key, target_date)
    if not flights_data:
        logging.warning(f"No flight data available for {target_date}. Terminating task.")
        return
        
    file_key = f"raw/flights/EHAM/{target_date}/arrivals.json"
    success = upload_to_minio(flights_data, "flight-weather-bronze", file_key, MINIO_CONN_ID)
    if not success:
        raise Exception("Failed to upload flight data to MinIO")

def extract_weather_task(**context):
    target_date = context['data_interval_start'].strftime("%Y-%m-%d")
    logging.info(f"Initiating weather extraction for {target_date}")
    
    weather_data = get_schiphol_weather(target_date)
    if not weather_data:
        logging.warning(f"No weather data available for {target_date}. Terminating task.")
        return
        
    file_key = f"raw/weather/EHAM/{target_date}/hourly.json"
    success = upload_to_minio(weather_data, "flight-weather-bronze", file_key, MINIO_CONN_ID)
    if not success:
        raise Exception("Failed to upload weather data to MinIO")

def load_flights_to_db_task(**context):
    target_date = context['data_interval_start'].strftime("%Y-%m-%d")
    file_key = f"raw/flights/EHAM/{target_date}/arrivals.json"
    
    success = load_json_to_postgres(
        target_date=target_date,
        minio_bucket="flight-weather-bronze",
        minio_key=file_key,
        pg_conn_id=DWH_CONN_ID,
        target_table="raw_flights",
        minio_conn_id=MINIO_CONN_ID
    )
    if not success:
        logging.warning(f"Failed to load {file_key} into PostgreSQL. File may be missing.")

def load_weather_to_db_task(**context):
    target_date = context['data_interval_start'].strftime("%Y-%m-%d")
    file_key = f"raw/weather/EHAM/{target_date}/hourly.json"
    
    success = load_json_to_postgres(
        target_date=target_date,
        minio_bucket="flight-weather-bronze",
        minio_key=file_key,
        pg_conn_id=DWH_CONN_ID,
        target_table="raw_weather",
        minio_conn_id=MINIO_CONN_ID
    )
    if not success:
        logging.warning(f"Failed to load {file_key} into PostgreSQL. File may be missing.")

with DAG(
    'schiphol_daily_extraction',
    default_args=default_args,
    description='ELT: Extract to MinIO -> Load to Postgres',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 2),
    catchup=True,
    tags=['bronze', 'silver', 'elt'],
) as dag:

    extract_flights_op = PythonOperator(
        task_id='extract_flights_to_minio',
        python_callable=extract_flights_task
    )
    extract_weather_op = PythonOperator(
        task_id='extract_weather_to_minio',
        python_callable=extract_weather_task
    )

    load_flights_op = PythonOperator(
        task_id='load_flights_to_dwh',
        python_callable=load_flights_to_db_task
    )
    load_weather_op = PythonOperator(
        task_id='load_weather_to_dwh',
        python_callable=load_weather_to_db_task
    )

    extract_flights_op >> load_flights_op
    extract_weather_op >> load_weather_op