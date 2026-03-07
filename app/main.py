"""
Main execution script for hourly alert processing.
"""

import logging
from typing import Any, Dict, List
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from app.database import is_first_deployment, mark_first_deployment_done
from app.database import get_latest_processed_date
from app.notification_service import send_notification
from app.database import delete_old_processed_alerts

from app.config import (
    load_environment,
    get_fyllo_base_url,
    get_database_url,
)
from app.database import (
    initialize_database,
    is_alert_processed,
    mark_alert_processed,
    get_connection,
)
from app.fyllo_client import FylloClient
from app.alert_processor import is_alert_valid


logging.basicConfig(
   level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def process_alerts(alerts: List[Dict[str, Any]], connection, plot_farmer_map: Dict[str, Dict[str, str]]) -> List[Dict[str, Any]]:
    grouped_alerts = defaultdict(list)
    messages_to_send: List[Dict[str, Any]] = []

    for alert in alerts:
        alert_id = alert["id"]

        if is_alert_processed(connection, alert_id):
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
            farmer = plot_farmer_map.get(plot_id, {})

            messages_to_send.append({
                "alert": alert,
                "farmer": farmer
            })

            mark_alert_processed(
                connection=connection,
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
        client = FylloClient(base_url)
        # Fetch plots and build plot → farmer mapping
        plots = client.fetch_plots()

        plot_farmer_map = {}

        for plot in plots:
            plot_id = plot.get("plotId")
            farmer_name = plot.get("farmerName")
            mobile_number = plot.get("farmerMobile")
            if mobile_number and not mobile_number.startswith("91"):
                mobile_number = "91" + mobile_number

            if plot_id:
                plot_farmer_map[plot_id] = {
                    "farmer_name": farmer_name,
                    "mobile_number": mobile_number,
                }

        database_url = get_database_url()
        if not database_url:
            raise ValueError("DATABASE_URL is not set")
        
        connection = get_connection(database_url)

        initialize_database(connection)

        last_date = get_latest_processed_date(connection)
        if last_date is not None:
            last_date = last_date.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

        all_alerts = []

        for plot in plots:
        
            plot_id = plot.get("plotId")

            if not plot_id:
                continue
            
            live_data = client.fetch_plot_live_data(plot_id, last_date)

            alerts = live_data.get("alerts", [])

            print(f"Plot {plot_id} alerts:", len(alerts))

            for alert in alerts:
                alert["plotId"] = plot_id
                all_alerts.append(alert)

        alerts = all_alerts
        print(f"Total alerts fetched: {len(alerts)}")

        # If DB is empty → first deployment
        if is_first_deployment(connection):
            print("First deployment detected. Marking existing alerts as processed without sending.")
            for alert in alerts:
                mark_alert_processed(
                    connection=connection,
                    alert_id=alert["id"],
                    plot_id=alert.get("plotId"),
                    alert_date=datetime.fromisoformat(alert["date"].replace("Z", "+00:00")),
                    notif_type_id=alert.get("notifTypeId"),
                )
            mark_first_deployment_done(connection)
            return
    
        messages = process_alerts(alerts, connection, plot_farmer_map)

        for alert_data in messages:
            send_notification(
                connection,
                alert_data["alert"],
                alert_data["farmer"]
            )

        # Maintenance cleanup
        delete_old_processed_alerts(connection, retention_days=60)

        connection.close()

    except Exception:
        logging.exception("Unexpected failure in alert service")


if __name__ == "__main__":
    main()