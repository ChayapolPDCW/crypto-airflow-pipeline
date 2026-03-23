from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2 # สำหรับเชื่อมต่อ Postgres (Supabase)
import os

# --- 1. การตั้งค่าการเชื่อมต่อ (ให้เอาจาก Supabase ของคุณมาใส่) ---
# รูปแบบ: postgresql://postgres:[PASSWORD]@db.[ID].supabase.co:5432/postgres
DB_URL = os.getenv('SUPABASE_DB_URL')


SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT']
# --- 2. ฟังก์ชันหลัก (Logic) ---
def fetch_and_save_multi_coins():
    prices_data = []
    
    # 2. Loop ดึงข้อมูลทีละเหรียญ
    for symbol in SYMBOLS:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        response = requests.get(url)
        data = response.json()
        prices_data.append((data['symbol'], float(data['price'])))

    # 3. เชื่อมต่อ Supabase เพื่อบันทึกแบบกลุ่ม (Bulk Insert)
    db_url = os.getenv('SUPABASE_DB_URL')
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    
    # ใช้ execute_values หรือ loop insert ก็ได้ครับ (ตัวอย่างแบบ Loop เพื่อให้เข้าใจง่าย)
    for coin_data in prices_data:
        cur.execute(
            "INSERT INTO crypto_prices (symbol, price_usd) VALUES (%s, %s)",coin_data
        )
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Successfully updated {len(prices_data)} coins!")
    
    
# --- 3. โครงสร้าง DAG (Manager) ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='crypto_to_supabase_v1',
    default_args=default_args,
    start_date=datetime(2026, 3, 22),
    schedule='*/5 * * * *', # รันทุกๆ 5 นาที
    catchup=False
) as dag:

    # --- 4. มอบหมายงานให้ Worker ---
    task_get_data = PythonOperator(
        task_id='fetch_and_save_data',
        python_callable=fetch_and_save_multi_coins
    )