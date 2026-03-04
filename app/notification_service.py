from typing import Dict


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