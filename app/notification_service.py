from datetime import datetime
from typing import Any, Dict, List, TypedDict

import requests

from app.config import (
    get_wati_api_token,
    get_wati_base_url,
    get_wati_template_name,
    get_wati_tenant_id,
    get_wati_test_number,
)
from app.database import mark_alert_processed
from app.exceptions import NotificationError

DEBUG_MODE = True


class AlertMeta(TypedDict, total=False):
    id: str
    notifTypeId: int
    date: str


class AlertPayload(TypedDict):
    alerts: List[AlertMeta]
    plotId: str
    text: str


class Farmer(TypedDict):
    farmer_name: str
    mobile_number: str


def build_notification_payload(
    alert: AlertPayload, farmer: Farmer, template_name: str
) -> Dict[str, Any]:
    """
    Build message payload independent of transport layer.
    """
    return {
        "template_name": template_name,
        "broadcast_name": "fyllo_alert_test_run",
        "parameters": [
            {"name": "1", "value": farmer.get("farmer_name") or " "},
            {"name": "2", "value": alert.get("plotId") or "Your Plot"},
            {"name": "3", "value": alert.get("text") or " "},
        ],
    }


def send_whatsapp_message(
    request_url: str, payload: Dict, headers: Dict[str, str]
) -> None:
    """
    Send message via WhatsApp using WATI API.

    Parameters
    ----------
    request_url : str
    payload : Dict[str, Any]
    headers : Dict[str, str]

    Returns
    -------
    None
    """
    try:
        response = requests.post(request_url, json=payload, headers=headers, timeout=10)

        if response.status_code != 200:
            raise NotificationError(
                f"WATI failed: {response.status_code} - {response.text}"
            )

    except requests.RequestException as exc:
        raise NotificationError(f"Request failed: {exc}")


def mark_alerts_processed(
    connection,
    alerts: List[AlertMeta],
    plot_id: str,
    alert_text: str,
) -> None:
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
            alert_date=(
                datetime.fromisoformat(a["date"].replace("Z", "+00:00"))
                if a.get("date")
                else datetime.now()
            ),
        )


def send_notification(
    connection,
    alert: AlertPayload,
    farmer: Farmer,
) -> None:
    """
    Send alert notification to farmer.

    Parameters
    ----------
    alert : Dict
        Alert payload.
    farmer : Dict
        Farmer details.
    wati_config : Dict[str, str]
        WATI configuration parameters.

    Returns
    -------
    None
    """
    wati_base_url = get_wati_base_url()
    wati_tenant_id = get_wati_tenant_id()
    wati_api_token = get_wati_api_token()
    wati_test_number = get_wati_test_number()
    wati_template_name = get_wati_template_name()

    url = f"{wati_base_url}/{wati_tenant_id}/api/v1/sendTemplateMessage"

    headers = {
        "Authorization": f"Bearer {wati_api_token}",
        "Content-Type": "application/json",
    }

    alerts: List[AlertMeta] = alert.get("alerts", [])
    mobile_number = farmer.get("mobile_number")
    alert_text = alert.get("text") or " "
    plot_id = alert.get("plotId")

    payload = build_notification_payload(
        alert=alert, farmer=farmer, template_name=wati_template_name
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
        send_whatsapp_message(request_url=request_url, payload=payload, headers=headers)

    mark_alerts_processed(
        connection=connection, alerts=alerts, plot_id=plot_id, alert_text=alert_text
    )
