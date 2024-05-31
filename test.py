import requests
def fetch_data(**kwargs):
    url = 'https://storage.googleapis.com/tao-isman/airflow/n_movies.csv'
    response = requests.get(url)
    data = response.json() 
    df = pd.DataFrame(data)
    df.to_csv('./n_movies.csv', index=False)

fetch_data()