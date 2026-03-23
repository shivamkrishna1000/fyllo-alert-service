import os
from datetime import UTC, datetime

from app.config import load_environment
from app.database import (
    delete_old_processed_alerts,
    get_connection,
    initialize_database,
    is_alert_processed,
)
from app.main import process_and_generate_notifications
from app.notification_service import send_notification


def test_process_alerts_flow():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    connection = get_connection(database_url)

    # Ensure table exists and is clean
    initialize_database(connection)
    delete_old_processed_alerts(connection, retention_days=0)

    # We will add test alerts
    now = datetime.now(UTC)

    alerts = [
        {
            "id": "alert-1",
            "plotId": "plot-A",
            "notifTypeId": 1,
            "text": "Irrigation required at your plot A",
            "date": now.isoformat(),
        },
        {
            "id": "alert-2",
            "plotId": "plot-A",
            "notifTypeId": 23,
            "text": "Rain alert",
            "date": now.isoformat(),
        },
    ]

    plot_sensor_map = {
        "plot-A": {
            "soilTemp": {"value": 25, "minOptimalValue": 15, "maxOptimalValue": 35},
            "moisture1": {"value": 3, "minOptimalValue": 5},
            "moisture2": {"value": 4, "minOptimalValue": 5},
        }
    }

    plot_weather_map = {
        "plot-A": {
            "dailyWeatherForecastData": [{"precipitationProbability": {"value": 70}}]
        }
    }

    plot_farmer_map = {
        "plot-A": {"farmer_name": "Test Farmer A", "mobile_number": "919999999999"},
        "plot-B": {"farmer_name": "Test Farmer B", "mobile_number": "918888888888"},
        "plot-C": {"farmer_name": "Test Farmer C", "mobile_number": "917777777777"},
    }

    # Call the function under test
    messages = process_and_generate_notifications(
        alerts, plot_weather_map, plot_sensor_map, connection, plot_farmer_map
    )

    assert len(messages) == 1

    for msg in messages:
        send_notification(connection, msg["alert"], msg["farmer"])

    message_text = messages[0]["alert"]["text"]
    alert_ids = messages[0]["alert"]["alerts"]
    assert len(alert_ids) == 2  # or expected count

    assert "Rain is expected in your area today" in message_text
    assert "Soil moisture is low, but rain is expected today" in message_text

    # Verify DB state
    assert is_alert_processed(connection, "alert-1") is True

    # All valid alerts should be stored
    assert is_alert_processed(connection, "alert-2") is True


def test_irrigation_with_high_rain_probability():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    connection = get_connection(database_url)

    initialize_database(connection)
    delete_old_processed_alerts(connection, retention_days=0)

    now = datetime.now(UTC)

    # ONLY irrigation alert (no rain alert)
    alerts = [
        {
            "id": "alert-10",
            "plotId": "plot-A",
            "notifTypeId": 1,
            "text": "Irrigation required",
            "date": now.isoformat(),
        }
    ]

    # Sensor condition → irrigation valid
    plot_sensor_map = {
        "plot-A": {
            "soilTemp": {"value": 25, "minOptimalValue": 15, "maxOptimalValue": 35},
            "moisture1": {"value": 2, "minOptimalValue": 5},
        }
    }

    # High rain probability (>60)
    plot_weather_map = {
        "plot-A": {
            "dailyWeatherForecastData": [{"precipitationProbability": {"value": 70}}]
        }
    }

    plot_farmer_map = {
        "plot-A": {"farmer_name": "Test Farmer", "mobile_number": "919999999999"}
    }

    messages = process_and_generate_notifications(
        alerts, plot_weather_map, plot_sensor_map, connection, plot_farmer_map
    )

    assert len(messages) == 1

    message_text = messages[0]["alert"]["text"]

    assert "rain is likely today" in message_text.lower()


def test_irrigation_with_low_rain_probability():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    connection = get_connection(database_url)

    initialize_database(connection)
    delete_old_processed_alerts(connection, retention_days=0)

    now = datetime.now(UTC)

    # ONLY irrigation alert (no rain alert)
    alerts = [
        {
            "id": "alert-20",
            "plotId": "plot-A",
            "notifTypeId": 1,
            "text": "Irrigation required",
            "date": now.isoformat(),
        }
    ]

    # Sensor condition → irrigation valid
    plot_sensor_map = {
        "plot-A": {
            "soilTemp": {"value": 25, "minOptimalValue": 15, "maxOptimalValue": 35},
            "moisture1": {"value": 2, "minOptimalValue": 5},
        }
    }

    # LOW rain probability (<30)
    plot_weather_map = {
        "plot-A": {
            "dailyWeatherForecastData": [{"precipitationProbability": {"value": 10}}]
        }
    }

    plot_farmer_map = {
        "plot-A": {"farmer_name": "Test Farmer", "mobile_number": "919999999999"}
    }

    messages = process_and_generate_notifications(
        alerts, plot_weather_map, plot_sensor_map, connection, plot_farmer_map
    )

    assert len(messages) == 1

    message_text = messages[0]["alert"]["text"].lower()

    assert "irrigation is recommended today" in message_text


def test_high_wind_alert():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    connection = get_connection(database_url)

    initialize_database(connection)
    delete_old_processed_alerts(connection, retention_days=0)

    now = datetime.now(UTC)

    # High wind alert only
    alerts = [
        {
            "id": "alert-30",
            "plotId": "plot-A",
            "notifTypeId": 24,
            "text": "High wind speed alert",
            "date": now.isoformat(),
        }
    ]

    # Sensors can be empty (wind doesn't need validation)
    plot_sensor_map = {"plot-A": {}}

    # Weather irrelevant
    plot_weather_map = {"plot-A": {}}

    plot_farmer_map = {
        "plot-A": {"farmer_name": "Test Farmer", "mobile_number": "919999999999"}
    }

    messages = process_and_generate_notifications(
        alerts, plot_weather_map, plot_sensor_map, connection, plot_farmer_map
    )

    assert len(messages) == 1

    message_text = messages[0]["alert"]["text"].lower()

    assert "avoid pesticide spraying" in message_text


def test_irrigation_sensor_validation_failure():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    connection = get_connection(database_url)

    initialize_database(connection)
    delete_old_processed_alerts(connection, retention_days=0)

    now = datetime.now(UTC)

    # Irrigation alert
    alerts = [
        {
            "id": "alert-40",
            "plotId": "plot-A",
            "notifTypeId": 1,
            "text": "Irrigation required",
            "date": now.isoformat(),
        }
    ]

    # ❌ Sensor condition NOT satisfied (moisture > optimal)
    plot_sensor_map = {"plot-A": {"moisture1": {"value": 10, "minOptimalValue": 5}}}

    # Weather irrelevant
    plot_weather_map = {"plot-A": {}}

    plot_farmer_map = {
        "plot-A": {"farmer_name": "Test Farmer", "mobile_number": "919999999999"}
    }

    messages = process_and_generate_notifications(
        alerts, plot_weather_map, plot_sensor_map, connection, plot_farmer_map
    )

    # ❌ No message should be generated
    assert len(messages) == 0

    # ❌ Alert should NOT be marked as processed
    from app.database import is_alert_processed

    assert is_alert_processed(connection, "alert-40") is False

    # ✅ Should be stored as rejected
    cursor = connection.cursor()
    cursor.execute(
        "SELECT reason, alert_text FROM rejected_alerts WHERE alert_id = %s",
        ("alert-40",),
    )
    result = cursor.fetchone()
    cursor.close()

    assert result is not None
    assert result[0] == "sensor_validation_failed"
    assert result[1] is not None
    assert result[1] != ""


def test_unsupported_alert_type():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    connection = get_connection(database_url)

    initialize_database(connection)
    delete_old_processed_alerts(connection, retention_days=0)

    now = datetime.now(UTC)

    # ❌ Unsupported alert type (e.g., 999)
    alerts = [
        {
            "id": "alert-50",
            "plotId": "plot-A",
            "notifTypeId": 999,
            "text": "Some random alert",
            "date": now.isoformat(),
        }
    ]

    # Sensors irrelevant
    plot_sensor_map = {"plot-A": {}}

    # Weather irrelevant
    plot_weather_map = {"plot-A": {}}

    plot_farmer_map = {
        "plot-A": {"farmer_name": "Test Farmer", "mobile_number": "919999999999"}
    }

    messages = process_and_generate_notifications(
        alerts, plot_weather_map, plot_sensor_map, connection, plot_farmer_map
    )

    # ❌ No message should be generated
    assert len(messages) == 0

    # ❌ Should NOT be marked as processed
    from app.database import is_alert_processed

    assert is_alert_processed(connection, "alert-50") is False

    # ✅ Should be stored as rejected
    cursor = connection.cursor()
    cursor.execute(
        "SELECT reason, alert_text FROM rejected_alerts WHERE alert_id = %s",
        ("alert-50",),
    )
    result = cursor.fetchone()
    cursor.close()

    assert result is not None
    assert result[0] == "unsupported_alert_type"
    assert result[1] is not None
    assert result[1] != ""


def test_high_soil_temp_with_rain_alert():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    connection = get_connection(database_url)

    initialize_database(connection)
    delete_old_processed_alerts(connection, retention_days=0)

    now = datetime.now(UTC)

    # High soil temp + rain alert
    alerts = [
        {
            "id": "alert-60",
            "plotId": "plot-A",
            "notifTypeId": 18,  # high soil temp
            "text": "High soil temperature",
            "date": now.isoformat(),
        },
        {
            "id": "alert-61",
            "plotId": "plot-A",
            "notifTypeId": 23,  # rain alert
            "text": "Rain alert",
            "date": now.isoformat(),
        },
    ]

    # Sensor condition → high soil temp VALID
    plot_sensor_map = {"plot-A": {"soilTemp": {"value": 40, "maxOptimalValue": 35}}}

    # Weather irrelevant since rain alert present
    plot_weather_map = {"plot-A": {}}

    plot_farmer_map = {
        "plot-A": {"farmer_name": "Test Farmer", "mobile_number": "919999999999"}
    }

    messages = process_and_generate_notifications(
        alerts, plot_weather_map, plot_sensor_map, connection, plot_farmer_map
    )

    assert len(messages) == 1

    message_text = messages[0]["alert"]["text"].lower()

    assert "soil temperature is high" in message_text
    assert "rain is expected today" in message_text


def test_duplicate_irrigation_alerts_same_plot():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    connection = get_connection(database_url)

    initialize_database(connection)
    delete_old_processed_alerts(connection, retention_days=0)

    now = datetime.now(UTC)

    # Two irrigation alerts for same plot
    alerts = [
        {
            "id": "alert-70",
            "plotId": "plot-A",
            "notifTypeId": 1,
            "text": "Irrigation required",
            "date": now.isoformat(),
        },
        {
            "id": "alert-71",
            "plotId": "plot-A",
            "notifTypeId": 1,
            "text": "Irrigation required again",
            "date": now.isoformat(),
        },
    ]

    # Valid irrigation condition
    plot_sensor_map = {"plot-A": {"moisture1": {"value": 2, "minOptimalValue": 5}}}

    # Low rain probability → irrigation recommended
    plot_weather_map = {
        "plot-A": {
            "dailyWeatherForecastData": [{"precipitationProbability": {"value": 10}}]
        }
    }

    plot_farmer_map = {
        "plot-A": {"farmer_name": "Test Farmer", "mobile_number": "919999999999"}
    }

    messages = process_and_generate_notifications(
        alerts, plot_weather_map, plot_sensor_map, connection, plot_farmer_map
    )

    assert len(messages) == 1

    for msg in messages:
        send_notification(connection, msg["alert"], msg["farmer"])

    message_text = messages[0]["alert"]["text"].lower()
    assert "irrigation is recommended today" in message_text

    # ✅ BOTH alerts should be marked processed
    assert is_alert_processed(connection, "alert-70") is True
    assert is_alert_processed(connection, "alert-71") is True
