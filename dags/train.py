from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'model_training_dag',
    default_args=default_args,
    description='A simple DAG to train a ML model',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['example'],
)

# Define the task functions
def extract_data(**kwargs):
    print("Extracting data...")
    # Simulate data extraction
    return "Data extracted"

def load_data(**kwargs):
    print("Loading data...")
    # Simulate data loading
    return "Data loaded"

def train_model(**kwargs):
    print("Training model...")
    # Simulate model training
    return "Model trained"

def notify_completion(**kwargs):
    print("Workflow completed successfully")
    # Simulate sending a notification
    return "Notification sent"

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    provide_context=True,
    python_callable=load_data,
    dag=dag,
)

train_task = PythonOperator(
    task_id='train_model',
    provide_context=True,
    python_callable=train_model,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='notify_completion',
    provide_context=True,
    python_callable=notify_completion,
    dag=dag,
)

# Define the task dependencies
extract_task >> load_task >> train_task >> notify_task
