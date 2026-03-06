from typing import Dict
from app.database import insert_sent_notification
from app.config import get_database_url

def send_notification(message: Dict) -> None:
    """
    Placeholder for WhatsApp (WATI) integration.
    Currently just prints.
    Later will call WATI API.
    """
    print("\n--- Sending Notification ---")
    print(f"Plot ID: {message['plot_id']}")
    print(f"Date: {message['date']}")
    print(f"Message: {message['message']}")

    database_url = get_database_url()

    insert_sent_notification(
        database_url=database_url,
        alert_id=message["alert_id"],
        farmer_name=message["farmer_name"],
        mobile_number=message["mobile_number"],
        plot_id=message["plot_id"],
        message=message["message"],
    )