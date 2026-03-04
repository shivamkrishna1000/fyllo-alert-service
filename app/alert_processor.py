"""
Alert processing logic.
"""

from datetime import datetime, UTC
from typing import Any, Dict


def is_alert_valid(alert: Dict[str, Any]) -> bool:
    """
    Check if alert is still valid (based on validTill field).
    """
    valid_till = alert.get("validTill")

    if valid_till is None:
        return True

    try:
        expiry_time = datetime.fromisoformat(
            valid_till.replace("Z", "+00:00")
        )
        current_time = datetime.now(UTC)
        return current_time < expiry_time
    except ValueError:
        return True


def simplify_alert_text(text: str) -> str:
    """
    Simplify alert text for farmer-friendly output.
    """

    if "Irrigation required" in text:
        return "Soil moisture is low. Please irrigate your plot today."

    if "Nutrient is required" in text:
        return "Nutrient application is required for your plot."

    if "High soil temperature" in text:
        return "Soil temperature is high. Monitor moisture levels carefully."

    return text