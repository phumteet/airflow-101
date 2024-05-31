from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
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
    'simple_dummy_dag',
    default_args=default_args,
    description='A simple dummy DAG for beginners',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
)

# Create a single DummyOperator task
dummy_task = DummyOperator(
    task_id='dummy_task',
    dag=dag,
)

# No dependencies since there's only one task
