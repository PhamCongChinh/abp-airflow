from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

from dags.utils.telegram_alert import task_fail_alert

def check_health_kafka():
    url = "http://192.168.1.28:4420/api/v1/check/kafka"

    res = requests.get(url, timeout=5)

    if res.status_code != 200:
        raise Exception(f"KAFKA lỗi status code: {res.status_code}")

    data = res.json()

    if data.get("status") != "OK":
        raise Exception(f"KAFKA không OK: {data}")

    print("KAFKA OK")

default_args = {
    "owner": "chinh",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="check_kafka",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/2 * * * *",  # mỗi 5 phút
    catchup=False,
) as dag:

    check_kafka = PythonOperator(
        task_id="check_health_kafka",
        python_callable=check_health_kafka,
        on_failure_callback=task_fail_alert
    )