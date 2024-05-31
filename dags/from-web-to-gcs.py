from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 31),
}
def fetch_data(**kwargs):
    url = 'https://storage.googleapis.com/tao-isman/airflow/n_movies.csv'
    response = requests.get(url)
    data = response.json() 
    df = pd.DataFrame(data)
    df.to_csv('./n_movies.csv', index=False)

with DAG(
    'website_data_to_gcs',
    default_args=default_args,
    description='Fetch data from a website, save to CSV, and upload to GCS',
    schedule_interval=timedelta(days=1),
) as dag:
    fetch_and_save_data = PythonOperator(
        task_id='fetch_and_save_data',
        python_callable=fetch_data
    )
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='./n_movies.csv',
        dst='isaman/data.csv',
        bucket='airflow-demo-odds',
        gcp_conn_id='google_cloud_default'
    )
    
fetch_and_save_data >> upload_to_gcs
