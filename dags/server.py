from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

from utils.telegram_alert import task_fail_alert
from utils.telegram_alert import send_telegram_alert

def check_health_server():
    url = "http://103.97.125.64:4416/api/v1/check/server"
    try:
        res = requests.get(url, timeout=5)

        data = res.json()
        cpu = data["cpu"]["percent"]
        ram = data["memory"]["percent"]
        disk = data["disk"]["percent"]

        print(f"CPU: {cpu}% | RAM: {ram}% | DISK: {disk}%")

        alerts = []

        if cpu > 90:
            alerts.append(f"CPU cao: {cpu}%")

        if ram > 90:
            alerts.append(f"RAM cao: {ram}%")

        if disk > 90:
            alerts.append(f"DISK đầy: {disk}%")

        if alerts:
            msg = "🚨 SERVER ALERT 🚨\n" + "\n".join(alerts)
            send_telegram_alert(msg)
            raise Exception(msg)

    except Exception as e:
        print("Health check failed:", str(e))
        raise

default_args = {
    "owner": "chinh",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="check_health_server",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/1 * * * *",  # mỗi 5 phút
    catchup=False,
) as dag:

    check_health = PythonOperator(
        task_id="check_health_server",
        python_callable=check_health_server,
    )