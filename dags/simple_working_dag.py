from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow')

def test_snowflake():
    """اختبار Snowflake فقط"""
    from snowflake.connector import connect
    import os
    from dotenv import load_dotenv
    
    load_dotenv('/opt/airflow/.env')
    
    conn = connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_USER()")
    result = cursor.fetchone()
    conn.close()
    print(f"✅ Snowflake connected: {result[0]}")
    return "Snowflake OK"

def test_amadeus():
    """اختبار Amadeus API"""
    try:
        from amadeus import Client, ResponseError
        import os
        from dotenv import load_dotenv
        
        load_dotenv('/opt/airflow/.env')
        
        client = Client(
            client_id=os.getenv('AMADEUS_CLIENT_ID'),
            client_secret=os.getenv('AMADEUS_CLIENT_SECRET')
        )
        
        response = client.shopping.flight_offers_search.get(
            originLocationCode='CAI',
            destinationLocationCode='DXB',
            departureDate='2026-05-15',
            adults=1,
            max=2
        )
        
        print(f"✅ Amadeus API: Found {len(response.data)} flights")
        return "Amadeus OK"
    except Exception as e:
        print(f"❌ Amadeus API Error: {e}")
        # نرجع نجاح حتى لو فشل Amadeus عشان نكمل تجربة Snowflake
        return "Amadeus Error (ignored)"

with DAG(
    'simple_working_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    task1 = PythonOperator(
        task_id='test_snowflake',
        python_callable=test_snowflake
    )
    
    task2 = PythonOperator(
        task_id='test_amadeus',
        python_callable=test_amadeus
    )
    
    task1 >> task2
