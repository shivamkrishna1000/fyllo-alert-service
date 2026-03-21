"""
Unit tests for alert processing logic.
"""

from datetime import datetime, timedelta, UTC

from app.alert_processor import is_alert_valid


def test_is_alert_valid_without_expiry():
    """
    GIVEN an alert without validTill
    WHEN checking validity
    THEN it should return True
    """
    alert = {"text": "Some alert"}

    assert is_alert_valid(alert) is True


def test_is_alert_valid_with_future_expiry():
    """
    GIVEN an alert with future validTill
    WHEN checking validity
    THEN it should return True
    """
    future_time = (
        datetime.now(UTC) + timedelta(hours=1)
    ).isoformat()

    alert = {"validTill": future_time}

    assert is_alert_valid(alert) is True


def test_is_alert_valid_with_past_expiry():
    """
    GIVEN an alert with past validTill
    WHEN checking validity
    THEN it should return False
    """
    past_time = (
        datetime.now(UTC) - timedelta(hours=1)
    ).isoformat()

    alert = {"validTill": past_time}

    assert is_alert_valid(alert) is False