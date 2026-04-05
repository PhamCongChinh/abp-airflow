import requests
import os

def send_telegram_alert(message):
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    print(BOT_TOKEN)
    print(CHAT_ID)

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": message
    })


def task_fail_alert(context):
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id

    message = f"❌ {dag_id} - {task_id} FAILED"
    send_telegram_alert(message)