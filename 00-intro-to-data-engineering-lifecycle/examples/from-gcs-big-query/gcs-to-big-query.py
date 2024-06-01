from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'clean_and_upload_to_bigquery',
    default_args=default_args,
    description='A DAG to clean data and load CSV from GCS to BigQuery',
    schedule_interval=None,  # Set as needed
    start_date=days_ago(1),
    catchup=False,
)

def clean_data():
    local_file_path = '/tmp/n_movies.csv'
    df = pd.read_csv(local_file_path)
    
    # Drop rows with missing title or description
    df = df.dropna(subset=['title', 'description'])
    
    # Fill missing values with default values
    df['year'] = df['year'].fillna('(Unknown)')
    df['certificate'] = df['certificate'].fillna('Not Rated')
    df['duration'] = df['duration'].fillna('Unknown')
    df['genre'] = df['genre'].fillna('Unknown')
    df['rating'] = df['rating'].fillna(0.0)
    df['votes'] = df['votes'].fillna('0')
    
    # Extract only the first year from the 'year' column
    df['year'] = df['year'].str.extract(r'(\d{4})', expand=False).fillna('Unknown')
    
    # Convert 'votes' to integers
    df['votes'] = df['votes'].str.replace(',', '').astype(int)
    
    # Save cleaned data to a new CSV file
    cleaned_file_path = '/tmp/cleaned_n_movies.csv'
    df.to_csv(cleaned_file_path, index=False)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

download_from_gcs = GCSToLocalFilesystemOperator(
    task_id='download_from_gcs',
    bucket='airflow-demo-odds',  # Replace with your GCS bucket name
    object_name='isaman/data.csv',  # The source object name
    filename='/tmp/n_movies.csv',
    dag=dag,
)

upload_cleaned_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_cleaned_to_gcs',
    src='/tmp/cleaned_n_movies.csv',
    dst='isaman/cleaned_n_movies.csv',
    bucket='airflow-demo-odds',  # Replace with your GCS bucket name
    mime_type='text/csv',
    dag=dag,
)

gcs_to_bq_task = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket='airflow-demo-odds',  # Replace with your GCS bucket name
    source_objects=['isaman/cleaned_n_movies.csv'],
    destination_project_dataset_table='airflow-class-424913.isaman_clean',  # Replace with your project, dataset, and table
    schema_fields=[
        {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'year', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'certificate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'duration', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'genre', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'rating', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'stars', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'votes', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    source_format='CSV',
    field_delimiter=',',
    skip_leading_rows=1,  # Adjust if your CSV has a header
    dag=dag,
)

download_from_gcs >> clean_data_task >> upload_cleaned_to_gcs >> gcs_to_bq_task
