"""
Database utility functions for storing processed alerts.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime, UTC


def initialize_database(database_url: str) -> None:
    """
    Create sent_alerts table if it does not exist.
    """
    connection = psycopg2.connect(database_url)
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_alerts (
            alert_id TEXT PRIMARY KEY,
            plot_id TEXT,
            notif_type_id INTEGER,
            alert_date TIMESTAMP,
            processed_at TIMESTAMP
        )
        """
    )
    connection.commit()
    cursor.close()
    connection.close()


def is_alert_processed(database_url: str, alert_id: str) -> bool:
    """
    Check whether an alert has already been processed.
    """
    connection = psycopg2.connect(database_url)
    cursor = connection.cursor()
    cursor.execute(
        "SELECT 1 FROM processed_alerts WHERE alert_id = %s",
        (alert_id,),
    )
    result = cursor.fetchone()

    cursor.close()
    connection.close()

    return result is not None


def mark_alert_processed(
    database_url: str,
    alert_id: str,
    plot_id: str,
    notif_type_id: int,
    alert_date: str,
) -> None:
    """
    Insert processed alert into database.
    """
    connection = psycopg2.connect(database_url)
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO processed_alerts 
            (alert_id, plot_id, notif_type_id, alert_date, processed_at)
            VALUES (%s, %s, %s, %s, NOW())
            """,
            (alert_id, plot_id, notif_type_id, alert_date),
        )
        connection.commit()

    except psycopg2.errors.UniqueViolation:
        connection.rollback()

    finally:
        cursor.close()
        connection.close()


def is_database_empty(database_url: str) -> bool:
    connection = psycopg2.connect(database_url)
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM processed_alerts")
    count = cursor.fetchone()[0]

    cursor.close()
    connection.close()

    return count == 0

def get_latest_processed_date(database_url: str) -> str | None:
    connection = psycopg2.connect(database_url)
    cursor = connection.cursor()

    cursor.execute("SELECT MAX(alert_date) FROM processed_alerts")
    result = cursor.fetchone()[0]

    cursor.close()
    connection.close()

    return result


def delete_old_processed_alerts(database_url: str, retention_days: int = 60) -> None:
    """
    Delete processed alerts older than retention_days.
    """
    connection = psycopg2.connect(database_url)
    cursor = connection.cursor()

    cursor.execute(
        """
        DELETE FROM processed_alerts
        WHERE processed_at < NOW() - INTERVAL %s
        """,
        (f"{retention_days} days",),
    )

    connection.commit()

    cursor.close()
    connection.close()