from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

from utils.telegram_alert import task_fail_alert

def check_health_api_master():
    url = "http://103.97.125.64:4416/api/v1/check/health"  # sửa lại URL của bạn

    res = requests.get(url, timeout=5)

    if res.status_code != 200:
        raise Exception(f"API lỗi status code: {res.status_code}")

    data = res.json()

    if data.get("status") != "OK":
        raise Exception(f"API không OK: {data}")

    print("API OK")



default_args = {
    "owner": "chinh",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="live_check_api_master",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/10 * * * *",  # mỗi 5 phút
    catchup=False,
) as dag:

    check_health = PythonOperator(
        task_id="live_check_health_api_master",
        python_callable=check_health_api_master,
        on_failure_callback=task_fail_alert
    )