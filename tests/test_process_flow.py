import os
from datetime import datetime, UTC, timedelta

from app.config import load_environment
from app.database import (
    initialize_database,
    delete_old_processed_alerts,
    is_alert_processed,
)
from app.main import process_alerts


def test_process_alerts_flow():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")

    # Ensure table exists and is clean
    initialize_database(database_url)
    delete_old_processed_alerts(database_url, retention_days=0)

    # We will add test alerts
    now = datetime.now(UTC)

    alerts = [
        # Valid new alert
        {
            "id": "alert-1",
            "plotId": "plot-A",
            "notifTypeId": 1,
            "text": "Irrigation required at your plot A",
            "date": now.isoformat(),
        },
        # Expired alert
        {
            "id": "alert-2",
            "plotId": "plot-A",
            "notifTypeId": 1,
            "text": "Irrigation required at your plot A",
            "date": now.isoformat(),
            "validTill": (now - timedelta(hours=1)).isoformat(),
        },
        # Already processed alert
        {
            "id": "alert-3",
            "plotId": "plot-B",
            "notifTypeId": 1,
            "text": "Irrigation required at your plot B",
            "date": now.isoformat(),
        },
        # Another valid new alert
        {
            "id": "alert-4",
            "plotId": "plot-C",
            "notifTypeId": 1,
            "text": "Irrigation required at your plot C",
            "date": now.isoformat(),
        },
    ]

    from app.database import mark_alert_processed

    mark_alert_processed(database_url=database_url, alert_id="alert-3", plot_id="plot-B", notif_type_id=1, alert_date=now)

    plot_farmer_map = {
        "plot-A": {
            "farmer_name": "Test Farmer A",
            "mobile_number": "919999999999"
        },
        "plot-B": {
            "farmer_name": "Test Farmer B",
            "mobile_number": "918888888888"
        },
        "plot-C": {
            "farmer_name": "Test Farmer C",
            "mobile_number": "917777777777"
        }
    }

    # Call the function under test
    messages = process_alerts(alerts, database_url, plot_farmer_map)

    # Only alert-1 and alert-4 should be processed
    assert len(messages) == 2

    returned_plots = {msg["plot_id"] for msg in messages}

    assert "plot-A" in returned_plots
    assert "plot-C" in returned_plots

    # Expired and already processed should NOT appear
    assert "plot-B" not in returned_plots

    # Verify DB state
    assert is_alert_processed(database_url, "alert-1") is True
    assert is_alert_processed(database_url, "alert-4") is True

    # Expired alert should not be stored
    assert is_alert_processed(database_url, "alert-2") is False