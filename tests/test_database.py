"""
Unit tests for database logic.
"""

import os
from datetime import UTC, datetime
from unittest.mock import MagicMock

from app.config import load_environment
from app.database import (
    delete_old_processed_alerts,
    get_connection,
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
    connection = get_connection(database_url)

    initialize_database(connection)
    delete_old_processed_alerts(connection, retention_days=0)

    alert_id = "test-alert-123"

    assert is_alert_processed(connection, alert_id) is False

    mark_alert_processed(
        connection=connection,
        alert_id=alert_id,
        plot_id="plot-1",
        alert_text="Test message",
        notif_type_id=1,
        alert_date=datetime.now(UTC),
    )

    assert is_alert_processed(connection, alert_id) is True

    connection.close()


def test_duplicate_alert_not_inserted():
    load_environment()
    database_url = os.environ.get("TEST_DATABASE_URL")
    connection = get_connection(database_url)

    initialize_database(connection)
    delete_old_processed_alerts(connection, retention_days=0)

    from datetime import UTC, datetime

    alert_id = "duplicate-test-alert"

    # Insert first time
    mark_alert_processed(
        connection=connection,
        alert_id=alert_id,
        plot_id="plot-1",
        alert_text="Test message",
        notif_type_id=1,
        alert_date=datetime.now(UTC),
    )

    # Insert second time (should NOT create new row)
    mark_alert_processed(
        connection=connection,
        alert_id=alert_id,
        plot_id="plot-1",
        alert_text="Test message",
        notif_type_id=1,
        alert_date=datetime.now(UTC),
    )

    cursor = connection.cursor()

    cursor.execute(
        "SELECT COUNT(*) FROM processed_alerts WHERE alert_id = %s",
        (alert_id,),
    )

    count = cursor.fetchone()[0]

    cursor.close()
    connection.close()

    assert count == 1


def test_delete_old_processed_alerts():
    mock_connection = MagicMock()

    delete_old_processed_alerts(mock_connection, 30)

    assert mock_connection.cursor.called
