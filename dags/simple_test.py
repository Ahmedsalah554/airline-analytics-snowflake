from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test():
    print("Hello from Airflow!")
    return "Success"

with DAG('test_simple', start_date=datetime(2024,1,1), schedule_interval=None) as dag:
    task = PythonOperator(task_id='test', python_callable=test)
