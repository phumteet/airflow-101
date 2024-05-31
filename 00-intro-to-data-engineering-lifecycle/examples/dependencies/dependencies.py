from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG('task_dependency_example',
         description='Simple Task Dependency Example',
         schedule_interval='@daily',
         start_date=datetime(2023, 1, 1),
         catchup=False) as dag:

    start_task = DummyOperator(task_id='start')
    
    # Run Python Function
    extract_data = BashOperator(
        task_id='extract_data',
        bash_command='echo "Extracting data..."',
    )

    # Python Task
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='echo "Transforming data..."',
    )

    load_data = BashOperator(
        task_id='load_data',
        bash_command='echo "Loading data..."',
    )

    end_task = DummyOperator(task_id='end')
    
    start_task >> extract_data >> transform_data >> load_data >> end_task

