import pandas as pd

def fetch_data(**kwargs):
    url = 'https://storage.googleapis.com/tao-isman/airflow/n_movies.csv'
    df = pd.read_csv(url)
    df.to_csv('./n_movies.csv', index=False)

fetch_data()
