# airflow-101
This repo is for sharing about basics apache airflow.

## prequisite
- docker desktop (or docker engine runnig)

## How to start
- create directories
  ```
  mkdir -p ./dags ./logs ./plugins ./config
  ```
- create environment file for store UID
  ```
  echo -e "AIRFLOW_UID=$(id -u)" > .env
  ```
- run docker init for Initialize the database
  ```
  docker compose up airflow-init
  ```
- run all airflow services
  ```
  docker compose up
  ```