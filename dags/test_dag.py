from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_connection():
    print(" Airflow is working!")
    return "Success"

with DAG(
    'test_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    test_task = PythonOperator(
        task_id='test_connection',
        python_callable=test_connection
    )
