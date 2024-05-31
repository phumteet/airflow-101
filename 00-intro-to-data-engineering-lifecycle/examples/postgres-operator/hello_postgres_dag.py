from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

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
    'hello_postgres_dag',
    default_args=default_args,
    description='A simple DAG with PostgresOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the task using PostgresOperator
create_table_task = PostgresOperator(
    task_id='create_table_task',
    postgres_conn_id='postgres_default',  # Connection ID configured in Airflow
    sql="""
    CREATE TABLE IF NOT EXISTS hello_data_engineer (
        id SERIAL PRIMARY KEY,
        message TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

insert_message_task = PostgresOperator(
    task_id='insert_message_task',
    postgres_conn_id='postgres_default',  # Connection ID configured in Airflow
    sql="""
    INSERT INTO hello_data_engineer (message) VALUES ('Hello, Data engineer!');
    """,
    dag=dag,
)

# Set task dependencies
create_table_task >> insert_message_task
