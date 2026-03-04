"""
Main execution script for hourly alert processing.
"""

import logging
from typing import Any, Dict, List
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from app.database import is_database_empty
from app.database import get_latest_processed_date
from app.notification_service import send_notification
from app.database import delete_old_processed_alerts

from app.config import (
    load_environment,
    get_fyllo_base_url,
    get_fyllo_token,
    get_database_url,
)
from app.database import (
    initialize_database,
    is_alert_processed,
    mark_alert_processed,
)
from app.fyllo_client import fetch_recent_alerts
from app.alert_processor import is_alert_valid, simplify_alert_text


"""logging.basicConfig(
   level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)"""

IST = timezone(timedelta(hours=5, minutes=30))

def convert_utc_to_ist(utc_string: str) -> str:
    dt = datetime.fromisoformat(utc_string.replace("Z", "+00:00"))
    dt_ist = dt.astimezone(IST)
    return dt_ist.strftime("%d-%m-%Y %I:%M %p")

def process_alerts(alerts: List[Dict[str, Any]], database_url: str) -> List[Dict[str, Any]]:
    grouped_alerts = defaultdict(list)
    messages_to_send: List[Dict[str, Any]] = []

    for alert in alerts:
        alert_id = alert["id"]

        if is_alert_processed(database_url, alert_id):
            continue

        if not is_alert_valid(alert):
            continue

        plot_id = alert.get("plotId")
        grouped_alerts[plot_id].append(alert)


    for plot_id, plot_alerts in grouped_alerts.items():
        print("\n----------------------------------")
        print(f"Plot: {plot_id}")
        print("----------------------------------")

        for alert in plot_alerts:
            simplified_text = simplify_alert_text(alert["text"])
            date_str = convert_utc_to_ist(alert["date"])

            messages_to_send.append({
                "plot_id": plot_id,
                "date": date_str,
                "message": simplified_text,
            })

            mark_alert_processed(
                database_url=database_url,
                alert_id=alert["id"],
                plot_id=plot_id,
                alert_date=datetime.fromisoformat(alert["date"].replace("Z", "+00:00")),
                notif_type_id=alert.get("notifTypeId"),
            )
    return messages_to_send


def main() -> None:
    """Entry point for hourly execution."""
    try:
        load_environment()

        base_url = get_fyllo_base_url()
        if not base_url:
            raise ValueError("FYLLO_BASE_URL is not set")
        token = get_fyllo_token()
        if not token:
            raise ValueError("FYLLO_TOKEN is not set")
        database_url = get_database_url()
        if not database_url:
            raise ValueError("DATABASE_URL is not set")

        initialize_database(database_url)

        last_date = get_latest_processed_date(database_url)
        if last_date is not None:
            last_date = last_date.isoformat()

        all_alerts = []
        skip = 0
        batch_size = 200  # increase from 100

        while True:
            alerts_batch = fetch_recent_alerts(
                base_url,
                token,
                since=last_date,
                limit=batch_size,
                skip=skip,
            )

            if not alerts_batch:
                break
            
            all_alerts.extend(alerts_batch)

            if len(alerts_batch) < batch_size:
                break
            
            skip += batch_size

        alerts = all_alerts

        # If DB is empty → first deployment
        if is_database_empty(database_url):
            print("First deployment detected. Marking existing alerts as processed without sending.")

            for alert in alerts:
                mark_alert_processed(
                    database_url=database_url,
                    alert_id=alert["id"],
                    plot_id=alert.get("plotId"),
                    alert_date=datetime.fromisoformat(alert["date"].replace("Z", "+00:00")),
                    notif_type_id=alert.get("notifTypeId"),
                )

            return
    
        messages = process_alerts(alerts, database_url)

        for message in messages:
            send_notification(message)

        # Maintenance cleanup
        delete_old_processed_alerts(database_url, retention_days=60)

    except Exception:
        logging.exception("Unexpected failure in alert service")


if __name__ == "__main__":
    main()