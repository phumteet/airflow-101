from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

# Function to decide which branch to take
def branch_func(**kwargs):
    condition = True  # Replace with your condition
    if condition:
        return 'success'
    else:
        return 'failure'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'example_branch_dag',
    default_args=default_args,
    description='An example DAG with branching',
    schedule_interval=None,
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=branch_func,
    )

    success = EmptyOperator(
        task_id='success',
    )

    failure = EmptyOperator(
        task_id='failure',
    )

    finish = EmptyOperator(
        task_id='finish',
        trigger_rule='none_failed_or_skipped',
    )

    send_error = EmptyOperator(
        task_id='send_error',
    )

    # Setting up dependencies
    start >> branching >> [success, failure] >> finish
    failure >> send_error

