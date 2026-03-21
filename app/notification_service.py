from typing import Dict, Any
from app.database import insert_sent_notification
import requests
DEBUG_MODE = True

from app.config import (
    get_wati_base_url,
    get_wati_tenant_id,
    get_wati_api_token,
    get_wati_test_number,
    get_wati_template_name
)
from app.database import mark_alert_processed
from datetime import datetime

def build_notification_payload(alert: Dict, farmer: Dict, template_name: str) -> Dict[str, Any]:
    """
    Build WATI template payload from alert and farmer data.
    """
    farmer_name = farmer.get("farmer_name")
    plot_id = alert.get("plotId")
    alert_text = alert.get("text") or " "

    parameters = [
        {"name": "1", "value": farmer_name or " "},
        {"name": "2", "value": plot_id or "Your Plot"},
        {"name": "3", "value": alert_text}
    ]

    return {
        "template_name": template_name,
        "broadcast_name": "fyllo_alert_test_run",
        "parameters": parameters
    }

def send_whatsapp_message(request_url: str, payload: Dict[str, Any], headers: Dict[str, str]) -> None:
    """
    Send message to WhatsApp via WATI API.
    """
    response = requests.post(
        request_url,
        json=payload,
        headers=headers,
        timeout=10
    )

    print("WATI Response:", response.status_code, response.text)

def mark_alerts_processed(connection, alerts, plot_id, alert_text):
    """
    Mark all alerts as processed in the database.
    """
    for a in alerts:
        mark_alert_processed(
            connection=connection,
            alert_id=a["id"],
            plot_id=plot_id,
            notif_type_id=a.get("notifTypeId"),
            alert_text=alert_text,
            alert_date=datetime.fromisoformat(
                a["date"].replace("Z", "+00:00")
            ) if a.get("date") else datetime.now()
        )

def send_notification(connection, alert: Dict, farmer: Dict) -> None:

    wati_base_url = get_wati_base_url()
    wati_tenant_id = get_wati_tenant_id()
    wati_api_token = get_wati_api_token()
    wati_test_number = get_wati_test_number()
    wati_template_name = get_wati_template_name()

    url = f"{wati_base_url}/{wati_tenant_id}/api/v1/sendTemplateMessage"

    headers = {
        "Authorization": f"Bearer {wati_api_token}",
        "Content-Type": "application/json"
    }

    alerts = alert.get("alerts", [])
    mobile_number = farmer.get("mobile_number")
    alert_text = alert.get("text") or " "
    plot_id = alert.get("plotId")

    payload = build_notification_payload(
        alert=alert,
        farmer=farmer,
        template_name=wati_template_name
    )

    request_url = f"{url}?whatsappNumber={wati_test_number}"

    if DEBUG_MODE:
        print("\n----- ALERT MESSAGE (DEBUG) -----")
        print("Farmer:", farmer.get("farmer_name"))
        print("Mobile:", mobile_number)
        print("Plot:", plot_id)
        print("Alert IDs:", [a["id"] for a in alerts])
        print("Message:", alert_text)
        print("---------------------------------\n")
    else:
        send_whatsapp_message(
            request_url=request_url,
            payload=payload,
            headers=headers
        )

    mark_alerts_processed(
        connection=connection,
        alerts=alerts,
        plot_id=plot_id,
        alert_text=alert_text
    )