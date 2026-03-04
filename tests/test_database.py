"""
Unit tests for database logic.
"""
import os
from app.config import load_environment
from datetime import datetime, UTC
from app.database import (
    initialize_database,
    is_alert_processed,
    mark_alert_processed,
)


def test_alert_mark_and_check():
    """
    GIVEN a fresh database
    WHEN marking alert as processed
    THEN is_alert_processed should return True
    """

    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")

    from app.database import delete_old_processed_alerts

    initialize_database(database_url)
    delete_old_processed_alerts(database_url, retention_days=0)

    alert_id = "test-alert-123"

    assert is_alert_processed(database_url, alert_id) is False

    mark_alert_processed(
        database_url=database_url,
        alert_id=alert_id,
        plot_id="plot-1",
        notif_type_id=1,
        alert_date=datetime.now(UTC),
    )

    assert is_alert_processed(database_url, alert_id) is True


def test_duplicate_alert_not_inserted():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")

    from app.database import delete_old_processed_alerts
    initialize_database(database_url)
    delete_old_processed_alerts(database_url, retention_days=0)

    from datetime import datetime, UTC

    alert_id = "duplicate-test-alert"

    # Insert first time
    mark_alert_processed(
        database_url=database_url,
        alert_id=alert_id,
        plot_id="plot-1",
        notif_type_id=1,
        alert_date=datetime.now(UTC),
    )

    # Insert second time (should NOT create new row)
    mark_alert_processed(
        database_url=database_url,
        alert_id=alert_id,
        plot_id="plot-1",
        notif_type_id=1,
        alert_date=datetime.now(UTC),
    )

    # Count rows with this alert_id
    import psycopg2

    connection = psycopg2.connect(database_url)
    cursor = connection.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM processed_alerts WHERE alert_id = %s",
        (alert_id,),
    )

    count = cursor.fetchone()[0]

    cursor.close()
    connection.close()

    assert count == 1