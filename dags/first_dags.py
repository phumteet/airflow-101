from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'first_dags',
    default_args=default_args,
    description='A simple empty DAG for beginners',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
)

# Create a single DummyOperator task
start_task = EmptyOperator(
    task_id='start_task',
    dag=dag,
)

start_task
