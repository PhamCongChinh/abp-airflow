from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

from utils.telegram_alert import task_fail_alert
from utils.telegram_alert import send_telegram_alert

def check_bot_health():

    url = f"http://192.168.1.28:4420/api/v1/check/bot-health"
    res = requests.get(url, timeout=5)

    data = res.json()

    print(data)

    dead = [b for b in data if b["status"] == "dead"]
    warning = [b for b in data if b["status"] == "warning"]

    msg_lines = []

    print(dead)
    print(warning)

    if dead:
        msg_lines.append("🚨 BOT DEAD:")
        for b in dead:
            msg_lines.append(
                f"- {b.get('bot_name')} ({b.get('bot_id')}) | {b.get('bot_type')}"
            )

    if warning:
        msg_lines.append("\n⚠️ BOT WARNING:")
        for b in warning:
            msg_lines.append(
                f"- {b.get('bot_name')} ({b.get('bot_id')}) | {b.get('bot_type')}"
            )

    print(msg_lines)

    if msg_lines:
        msg = "\n".join(msg_lines)

        send_telegram_alert(msg)

        raise Exception(msg)  # để Airflow fail
    
default_args = {
    "owner": "chinh",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="check_bot_health",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/1 * * * *",  # mỗi 5 phút
    catchup=False,
) as dag:

    check_health = PythonOperator(
        task_id="check_bot_health",
        python_callable=check_bot_health
    )