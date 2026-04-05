from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests


def send_test_telegram():
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    if not BOT_TOKEN or not CHAT_ID:
        raise Exception("Thiếu TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    message = "🚀 Test Airflow gửi Telegram thành công!"

    response = requests.post(
        url,
        json={
            "chat_id": CHAT_ID,
            "text": message
        },
        timeout=5
    )

    if response.status_code != 200:
        raise Exception(f"Lỗi gửi Telegram: {response.text}")


with DAG(
    dag_id="test_telegram_alert",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # 👈 chỉ chạy manual
    catchup=False,
    tags=["test", "telegram"],
) as dag:

    test_telegram_task = PythonOperator(
        task_id="send_test_message",
        python_callable=send_test_telegram,
    )