import requests
import os
from datetime import timedelta

def send_telegram_alert(message):
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    })


def task_fail_alert(context):
    task_instance = context["task_instance"]

    task_id = task_instance.task_id
    dag_id = task_instance.dag_id

    execution_date = context.get("execution_date")
    vn_time = execution_date + timedelta(hours=7)

    log_url = task_instance.log_url
    try_number = task_instance.try_number
    max_tries = task_instance.max_tries

    exception = context.get("exception")

    message = f"""
🔥 Airflow Cảnh báo
📌 *DAG*: `{dag_id}`
🔧 *Task*: `{task_id}`
🕒 Time (VN): {vn_time.strftime('%d-%m-%Y %H:%M:%S')}
❗ Error: {str(exception)[:200]}
    """

    send_telegram_alert(message)