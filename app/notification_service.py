from typing import Dict
from app.database import insert_sent_notification

def send_notification(connection, alert: Dict, farmer: Dict) -> None:
    """
    Placeholder for WhatsApp (WATI) integration.
    Currently just prints.
    Later will call WATI API.
    """
    farmer_name = farmer.get("farmer_name")
    mobile_number = farmer.get("mobile_number")
    plot_id = alert.get("plotId")
    alert_id = alert.get("id")

    print("\n--- Sending Notification ---")
    print(f"Plot ID: {plot_id}")
    print(f"Date: {alert['date']}")
    print(f"Message: {alert['text']}")

    insert_sent_notification(
        connection=connection,
        alert_id=alert_id,
        farmer_name=farmer_name,
        mobile_number=mobile_number,
        plot_id=plot_id,
        message=alert["text"],
    )