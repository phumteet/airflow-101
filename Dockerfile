FROM apache/airflow:slim-2.9.0
ADD requirements.txt .
RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r requirements.txt