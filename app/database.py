"""
Database utility functions for storing processed alerts.
"""

import psycopg2


def get_connection(database_url: str):
    """
    Create a database connection.
    """
    return psycopg2.connect(database_url)


def initialize_database(connection) -> None:
    """
    Create sent_alerts table if it does not exist.
    """
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_alerts (
            alert_id TEXT PRIMARY KEY,
            plot_id TEXT,
            notif_type_id INTEGER,
            alert_text TEXT,
            alert_date TIMESTAMP,
            processed_at TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sent_notifications (
            id SERIAL PRIMARY KEY,
            alert_id TEXT,
            farmer_name TEXT,
            mobile_number TEXT,
            plot_id TEXT,
            message TEXT,
            sent_at TIMESTAMP
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS rejected_alerts (
            id SERIAL PRIMARY KEY,
            alert_id TEXT,
            plot_id TEXT,
            notif_type_id INTEGER,
            reason TEXT,
            alert_text TEXT,
            alert_date TIMESTAMP,
            rejected_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (alert_id, reason)
        )
        """
    )

    connection.commit()
    cursor.close()


def is_alert_processed(connection, alert_id: str) -> bool:
    """
    Check whether an alert has already been processed.
    """
    cursor = connection.cursor()
    cursor.execute(
        "SELECT 1 FROM processed_alerts WHERE alert_id = %s",
        (alert_id,),
    )
    result = cursor.fetchone()

    cursor.close()

    return result is not None


def mark_alert_processed(connection, alert_id: str, plot_id: str,notif_type_id: int, alert_text: str, alert_date: str) -> None:
    """
    Insert processed alert into database.
    """
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO processed_alerts 
            (alert_id, plot_id, notif_type_id, alert_text, alert_date, processed_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (alert_id, plot_id, notif_type_id, alert_text, alert_date),
        )
        connection.commit()

    except psycopg2.errors.UniqueViolation:
        connection.rollback()

    finally:
        cursor.close()


def insert_rejected_alert(connection, alert_id: str, plot_id: str, notif_type_id: int, reason: str, alert_text: str, alert_date) -> None:
    """
    Insert a rejected alert into rejected_alerts table.
    """
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO rejected_alerts
            (alert_id, plot_id, notif_type_id, reason, alert_text, alert_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (alert_id, plot_id, notif_type_id, reason, alert_text, alert_date),
        )
        connection.commit()

    except psycopg2.errors.UniqueViolation:
        connection.rollback()

    finally:
        cursor.close()


def get_latest_processed_date(connection) -> str | None:
    cursor = connection.cursor()

    cursor.execute("SELECT MAX(alert_date) FROM processed_alerts")
    result = cursor.fetchone()[0]

    cursor.close()

    return result


def delete_old_processed_alerts(connection, retention_days: int = 60) -> None:
    """
    Delete processed alerts older than retention_days.
    """
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


def insert_sent_notification(connection, alert_id: str, farmer_name: str, mobile_number: str, plot_id: str, message: str) -> None:

    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO sent_notifications
        (alert_id, farmer_name, mobile_number, plot_id, message, sent_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        """,
        (alert_id, farmer_name, mobile_number, plot_id, message),
    )

    connection.commit()

    cursor.close()

def get_processed_alert_ids(connection):
    """
    Fetch all processed alert IDs from database.
    """
    cursor = connection.cursor()

    cursor.execute("SELECT alert_id FROM processed_alerts")

    rows = cursor.fetchall()

    cursor.close()

    return {row[0] for row in rows}