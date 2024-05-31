from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'first_dag',
    default_args=default_args,
    description='My first Airflow DAG with an empty operator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

start >> end