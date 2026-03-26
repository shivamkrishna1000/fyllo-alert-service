"""
Main execution script for daily alert processing.
"""

import logging
from datetime import timezone
from typing import Any, Dict, List, Tuple, TypedDict

from app.alert_processor import process_and_generate_notifications
from app.config import get_database_url, get_fyllo_base_url, load_environment
from app.database import (
    delete_old_processed_alerts,
    get_connection,
    get_latest_processed_date,
    initialize_database,
)
from app.exceptions import (
    DatabaseError,
    FylloAPIError,
    FylloAuthError,
    NotificationError,
)
from app.fyllo_client import FylloClient
from app.notification_service import send_notification


class Plot(TypedDict, total=False):
    plotId: str
    farmerName: str
    farmerMobile: str


class Farmer(TypedDict):
    farmer_name: str
    mobile_number: str


class Alert(TypedDict, total=False):
    id: str
    plotId: str
    notifTypeId: int
    text: str
    date: str


class PlotMessage(TypedDict):
    alert: Dict
    farmer: Farmer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_plot_farmer_map(plots: List[Plot]) -> Dict[str, Farmer]:
    """
    Build mapping of plot IDs to farmer details.

    Parameters
    ----------
    plots : List[Dict]
        List of plot data from API.

    Returns
    -------
    Dict[str, Dict[str, str]]
        Mapping of plot_id → farmer details (name, mobile number).
    """
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


def fetch_plot_data(
    client: FylloClient, plots: List[Plot], last_date: str | None
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Fetch live sensor data and weather data for all plots.

    Parameters
    ----------
    client : FylloClient
        API client instance.
    plots : List[Dict]
        List of plot metadata.
    last_date : str | None
        Last processed alert timestamp.

    Returns
    -------
    Tuple[Dict[str, Any], Dict[str, Any]]
        (plot_live_data_map, plot_weather_map)
    """
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


def build_alert_context(
    plots: List[Plot], plot_live_data_map: Dict[str, Dict]
) -> Tuple[List[Alert], Dict[str, Any]]:
    """
    Extract alerts and sensor data from API response.

    Parameters
    ----------
    plots : List[Dict]
        List of plots.
    plot_live_data_map : Dict[str, Dict]
        Mapping of plot_id → live data.

    Returns
    -------
    Tuple[List[Dict], Dict[str, Any]]
        (alerts, plot_sensor_map)
    """
    all_alerts: List[Alert] = []
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


def send_notifications(connection, messages: List[PlotMessage]) -> None:
    """
    Send notifications for all generated messages.

    Parameters
    ----------
    connection
        Database connection.
    messages : List[Dict]
        List of alert messages with farmer details.

    Returns
    -------
    None
    """
    for alert_data in messages:
        send_notification(connection, alert_data["alert"], alert_data["farmer"])


def run_pipeline(
    client: FylloClient,
    connection,
    plots: List[Plot],
    plot_farmer_map: Dict[str, Farmer],
) -> None:
    """
    Execute full alert processing pipeline.

    Steps:
    1. Fetch latest processed timestamp
    2. Fetch plot data from API
    3. Extract alerts and sensor data
    4. Process alerts and generate messages
    5. Send notifications
    6. Cleanup old records

    Parameters
    ----------
    client : FylloClient
    connection
    plots : List[Dict]
    plot_farmer_map : Dict[str, Dict[str, str]]

    Returns
    -------
    None
    """
    last_date = get_latest_processed_date(connection)

    if last_date:
        last_date = (
            last_date.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        )

    plot_live_data_map, plot_weather_map = fetch_plot_data(client, plots, last_date)

    alerts, plot_sensor_map = build_alert_context(plots, plot_live_data_map)

    messages = process_and_generate_notifications(
        alerts, plot_weather_map, plot_sensor_map, connection, plot_farmer_map
    )

    send_notifications(connection, messages)

    delete_old_processed_alerts(connection, retention_days=60)


def main():
    """
    Entry point for alert processing service.

    Initializes environment, database, API client,
    and runs the processing pipeline.
    """
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
            plot_farmer_map=plot_farmer_map,
        )

    except (FylloAPIError, FylloAuthError) as e:
        logging.error("Fyllo error: %s", e)

    except DatabaseError as e:
        logging.error("Database error: %s", e)

    except NotificationError as e:
        logging.error("Notification error: %s", e)

    except Exception as e:
        logging.exception("Unexpected failure: %s", e)

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
