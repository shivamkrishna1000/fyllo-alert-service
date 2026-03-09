from typing import Dict
from app.database import insert_sent_notification
from app.alert_processor import simplify_alert_text
import requests

from app.config import (
    get_wati_base_url,
    get_wati_tenant_id,
    get_wati_api_token,
    get_wati_test_number,
    get_wati_template_name
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

    farmer_name = farmer.get("farmer_name")
    mobile_number = farmer.get("mobile_number")
    plot_id = alert.get("plotId")
    alert_id = alert.get("id")
    raw_text = alert.get("text")
    alert_text = simplify_alert_text(raw_text) if raw_text else " "

    parameters = [
        {"name": "1", "value": farmer_name or " "},
        {"name": "2", "value": plot_id or "Your Plot"},
        {"name": "3", "value": alert_text or " "}
    ]

    payload = {
        "template_name": wati_template_name,
        "broadcast_name": "fyllo_alert_test_run",
        "parameters": parameters
    }

    request_url = f"{url}?whatsappNumber={wati_test_number}"

    response = requests.post(
        request_url,
        json=payload,
        headers=headers,
        timeout=10
    )

    print("WATI Response:", response.status_code, response.text)

    insert_sent_notification(
        connection=connection,
        alert_id=alert_id,
        farmer_name=farmer_name,
        mobile_number=mobile_number,
        plot_id=plot_id,
        message=alert_text,
    )