"""
Main execution script for hourly alert processing.
"""
import logging
from datetime import timezone
from app.database import get_latest_processed_date
from app.notification_service import send_notification
from app.database import delete_old_processed_alerts
from app.alert_processor import process_alerts
from typing import List, Dict, Tuple, Any

from app.config import (
    load_environment,
    get_fyllo_base_url,
    get_database_url,
)
from app.database import (
    initialize_database,
    get_connection,
)
from app.fyllo_client import FylloClient

logging.basicConfig(
   level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def get_plot_farmer_map(plots: List[Dict]) -> Dict[str, Dict[str, str]]:

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

    return plot_farmer_map

def fetch_plot_data(client: FylloClient, plots: List[Dict], last_date: str | None) -> Tuple[Dict[str, Any], Dict[str, Any]]:

    plot_live_data_map = {}
    plot_weather_map = {}

    for plot in plots:
        plot_id = plot.get("plotId")

        if not plot_id:
            continue

        live_data = client.fetch_plot_live_data(plot_id, last_date)
        weather_data = client.fetch_weather_forecast(plot_id)

        plot_live_data_map[plot_id] = live_data
        plot_weather_map[plot_id] = weather_data

    return plot_live_data_map, plot_weather_map

def build_alert_context(plots: List[Dict], plot_live_data_map: Dict[str, Dict]) -> Tuple[List[Dict], Dict[str, Any]]:
    """
    Transform raw API data into alerts, sensor map, and weather map.
    Pure function (no API calls).
    """

    all_alerts = []
    plot_sensor_map = {}

    for plot in plots:
        plot_id = plot.get("plotId")

        if not plot_id:
            continue

        live_data = plot_live_data_map.get(plot_id, {})

        sensors = live_data.get("sensors", {})
        plot_sensor_map[plot_id] = sensors

        alerts = live_data.get("alerts", [])

        for alert in alerts:
            alert["plotId"] = plot_id
            all_alerts.append(alert)

    return all_alerts, plot_sensor_map

def send_notifications(connection, messages: List[Dict]) -> None:
    """
    Send all generated alert messages to farmers.
    """
    for alert_data in messages:
        send_notification(connection, alert_data["alert"], alert_data["farmer"])

def run_pipeline(client: FylloClient, connection, plots: List[Dict], plot_farmer_map: Dict[str, Dict[str, str]]) -> None:
    """
    Execute full alert processing pipeline.
    """

    last_date = get_latest_processed_date(connection)

    if last_date:
        last_date = (last_date.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))

    plot_live_data_map, plot_weather_map = fetch_plot_data(client, plots, last_date)

    alerts, plot_sensor_map = build_alert_context(plots, plot_live_data_map)

    logging.info("Total alerts fetched: %d", len(alerts))

    messages = process_alerts(alerts, plot_weather_map, plot_sensor_map, connection, plot_farmer_map)

    send_notifications(connection, messages)

    delete_old_processed_alerts(connection, retention_days=60)

def main():
    connection = None

    try:
        load_environment()

        base_url = get_fyllo_base_url()
        if not base_url:
            raise ValueError("FYLLO_BASE_URL is not set")
        
        database_url = get_database_url()
        if not database_url:
            raise ValueError("DATABASE_URL is not set")
        
        connection = get_connection(database_url)
        initialize_database(connection)

        client = FylloClient(base_url)

        plots = client.fetch_plots()
        plot_farmer_map = get_plot_farmer_map(plots)

        run_pipeline(
            client=client,
            connection=connection,
            plots=plots,
            plot_farmer_map=plot_farmer_map
        )

    except Exception:
        logging.exception("Unexpected failure in alert service")

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()