"""
Alert processing logic.
"""

from datetime import datetime, UTC
from typing import List, Any, Dict
from collections import defaultdict
from app.database import (
    insert_rejected_alert,
    get_processed_alert_ids
)
import json
import os

from typing import TypedDict, List, Dict, Any

class Alert(TypedDict, total=False):
    id: str
    plotId: str
    notifTypeId: int
    text: str
    date: str
    validTill: str

class RejectedAlert(TypedDict):
    alert_id: str
    plot_id: str
    notif_type_id: int
    reason: str
    alert_text: str
    alert_date: Any

class Farmer(TypedDict, total=False):
    farmer_name: str
    mobile_number: str

class PlotMessage(TypedDict):
    alert: Dict[str, Any]
    farmer: Farmer


def load_rules():
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, "rules.json")

    with open(file_path, "r") as f:
        return json.load(f)
    
def is_supported_alert(notif_type_id: int) -> bool:
    return notif_type_id in ALERT_TYPE_MAP


def is_duplicate_alert(alert_id: str, processed_alert_ids: set) -> bool:
    return alert_id in processed_alert_ids


def is_expired_alert(alert: Dict[str, Any]) -> bool:
    return not is_alert_valid(alert)


def build_rejected_alert(alert: Alert, reason: str) -> RejectedAlert:
    return {
        "alert_id": alert["id"],
        "plot_id": alert.get("plotId"),
        "notif_type_id": alert.get("notifTypeId"),
        "reason": reason,
        "alert_text": alert.get("text", ""),
        "alert_date": datetime.fromisoformat(
            alert["date"].replace("Z", "+00:00")
        ),
    }

def filter_and_validate_alerts(
    alerts: List[Alert],
    plot_sensor_map: Dict[str, Any],
    processed_alert_ids: set
) -> tuple[Dict[str, List[Alert]], List[RejectedAlert]]:
    """
    Filter and validate incoming alerts.

    Applies:
    - Supported alert type check
    - Duplicate alert check
    - Expiry validation
    - Sensor validation

    Parameters
    ----------
    alerts : List[Dict]
        Raw alerts
    plot_sensor_map : Dict
        Sensor data for each plot
    processed_alert_ids : set
        Set of already processed alert IDs

    Returns
    -------
    Tuple[Dict[str, List[Dict]], List[Dict]]
        grouped_alerts : valid alerts grouped by plot
        rejected_alerts : list of rejected alerts with reasons
    """
    grouped_alerts = defaultdict(list)
    rejected_alerts = []

    for alert in alerts:
        alert_id = alert["id"]
        plot_id = alert.get("plotId")
        notif_type_id = alert.get("notifTypeId")

        print("Checking alert:", alert_id)

        if not is_supported_alert(notif_type_id):
            rejected_alerts.append(build_rejected_alert(alert, "unsupported_alert_type"))
            continue

        if is_duplicate_alert(alert_id, processed_alert_ids):
            rejected_alerts.append(build_rejected_alert(alert, "duplicate_alert"))
            continue

        if is_expired_alert(alert):
            rejected_alerts.append(build_rejected_alert(alert, "expired_alert"))
            continue

        sensors = plot_sensor_map.get(plot_id)
        if not validate_sensor_conditions(alert, sensors or {}):
            rejected_alerts.append(build_rejected_alert(alert, "sensor_validation_failed"))
            continue

        grouped_alerts[plot_id].append(alert)

    return grouped_alerts, rejected_alerts

def combine_messages(messages: List[str]) -> str:
    """
    Combine multiple advisory messages into a single string.
    """
    return "\n\n".join(messages)

def format_plot_message(plot_id: str, plot_alerts: List[Dict], message: str, farmer: Dict) -> Dict:
    """
    Format final message structure for notification service.
    """
    return {
        "alert": {
            "alerts": [
                {
                    "id": alert["id"],
                    "notifTypeId": alert.get("notifTypeId"),
                    "date": alert.get("date")
                }
                for alert in plot_alerts
            ],
            "plotId": plot_id,
            "text": message
        },
        "farmer": farmer
    }

def build_single_plot_message(plot_id: str, plot_alerts: List[Alert], weather_data: Dict[str, Any], farmer: Farmer) -> PlotMessage | None:
    """
    Generate message for a single plot.
    """
    advisory_messages = evaluate_rules(plot_alerts, weather_data)

    if not advisory_messages:
        return None

    combined_message = combine_messages(advisory_messages)

    return format_plot_message(
        plot_id=plot_id,
        plot_alerts=plot_alerts,
        message=combined_message,
        farmer=farmer
    )

def build_messages_for_plots(grouped_alerts: Dict[str, List[Alert]], plot_weather_map: Dict[str, Any], plot_farmer_map: Dict[str, Farmer],) -> List[PlotMessage]:
    """
    Build notification messages for each plot using rule engine.
    """

    messages_to_send: List[PlotMessage] = []

    for plot_id, plot_alerts in grouped_alerts.items():

        print("\n----------------------------------")
        print(f"Plot: {plot_id}")
        print("----------------------------------")

        weather_data = plot_weather_map.get(plot_id)
        farmer = plot_farmer_map.get(plot_id, {})

        message = build_single_plot_message(
            plot_id,
            plot_alerts,
            weather_data,
            farmer
        )

        if message:
            messages_to_send.append(message)

    return messages_to_send

def process_alerts(
    alerts: List[Alert],
    plot_weather_map: Dict[str, Any],
    plot_sensor_map: Dict[str, Any],
    connection,
    plot_farmer_map: Dict[str, Farmer]
) -> List[PlotMessage]:
    """
    Main orchestration function for alert processing.

    Steps:
    1. Fetch processed alert IDs from database
    2. Filter and validate incoming alerts
    3. Store rejected alerts
    4. Generate advisory messages for valid alerts

    Parameters
    ----------
    alerts : List[Dict]
        Raw alerts fetched from Fyllo API
    plot_weather_map : Dict
        Weather forecast data for each plot
    plot_sensor_map : Dict
        Sensor readings for each plot
    connection :
        Database connection object
    plot_farmer_map : Dict
        Mapping of plot_id to farmer details

    Returns
    -------
    List[Dict]
        List of messages ready to be sent to farmers
    """

    processed_alert_ids = get_processed_alert_ids(connection)

    grouped_alerts, rejected_alerts = filter_and_validate_alerts(
        alerts=alerts,
        plot_sensor_map=plot_sensor_map,
        processed_alert_ids=processed_alert_ids
    )

    for rejected in rejected_alerts:
        insert_rejected_alert(
            connection=connection,
            alert_id=rejected["alert_id"],
            plot_id=rejected["plot_id"],
            notif_type_id=rejected["notif_type_id"],
            reason=rejected["reason"],
            alert_text=rejected["alert_text"],
            alert_date=rejected["alert_date"],
        )

    messages_to_send = build_messages_for_plots(
        grouped_alerts=grouped_alerts,
        plot_weather_map=plot_weather_map,
        plot_farmer_map=plot_farmer_map,
    )

    return messages_to_send


ALERT_TYPE_MAP = {
    1: "irrigation",
    17: "low_soil_temp",
    18: "high_soil_temp",
    23: "rain_alert",
    24: "high_wind"
}

TRIGGER_PRIORITY = {
    "rain_alert": 1,
    "irrigation": 2,
    "nutrient": 3,
    "low_soil_temp": 4,
    "high_soil_temp": 4,
    "high_wind": 5
}

RULE_TABLE = load_rules()
RULES_BY_TRIGGER = {}

for rule in RULE_TABLE:
    trigger = rule["trigger"]

    if trigger not in RULES_BY_TRIGGER:
        RULES_BY_TRIGGER[trigger] = []

    RULES_BY_TRIGGER[trigger].append(rule)

CONDITIONS = {
    "always": lambda ctx: True,
    "rain_alert": lambda ctx: ctx.get("rain_alert"),
    "rain_prob_gt_60": lambda ctx: ctx.get("rain_prob_gt_60"),
    "rain_prob_30_60": lambda ctx: ctx.get("rain_prob_30_60"),
    "rain_prob_lt_30": lambda ctx: ctx.get("rain_prob_lt_30"),
}

def validate_sensor_conditions(alert, sensors):

    notif_type = alert.get("notifTypeId")

    soil_temp = sensors.get("soilTemp")
    moisture1 = sensors.get("moisture1")
    moisture2 = sensors.get("moisture2")

    # ---------- IRRIGATION ----------
    if notif_type == 1:

        if not moisture1 and not moisture2:
            return False

        if moisture1:
            value = moisture1.get("value")
            min_opt = moisture1.get("minOptimalValue")

            if value is not None and min_opt is not None:
                if float(value) < float(min_opt):
                    return True

        if moisture2:
            value = moisture2.get("value")
            min_opt = moisture2.get("minOptimalValue")

            if value is not None and min_opt is not None:
                if float(value) < float(min_opt):
                    return True

        return False


    # ---------- LOW SOIL TEMP ----------
    if notif_type == 17:

        if not soil_temp:
            return False

        value = soil_temp.get("value")
        min_opt = soil_temp.get("minOptimalValue")

        if value is None or min_opt is None:
            return False

        return float(value) < float(min_opt)


    # ---------- HIGH SOIL TEMP ----------
    if notif_type == 18:

        if not soil_temp:
            return False

        value = soil_temp.get("value")
        max_opt = soil_temp.get("maxOptimalValue")

        if value is None or max_opt is None:
            return False

        return float(value) > float(max_opt)


    # ---------- WEATHER ALERTS ----------
    # Rain / Wind do not require sensor validation
    return True

def build_context(plot_alerts: List[Alert], weather_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build rule evaluation context from alerts and weather.

    Creates boolean flags such as:
    - irrigation
    - rain_alert
    - rain probability categories

    Parameters
    ----------
    plot_alerts : List[Dict]
        Alerts for a plot
    weather_data : Dict
        Weather forecast data

    Returns
    -------
    Dict
        Context dictionary used by rule engine
    """

    context = {
        "irrigation": False,
        "rain_alert": False,
        "low_soil_temp": False,
        "high_soil_temp": False,
        "high_wind": False,
        "rain_prob_gt_60": False,
        "rain_prob_30_60": False,
        "rain_prob_lt_30": False,
    }

    rain_probability = None

    if weather_data:
        daily = weather_data.get("dailyWeatherForecastData", [])

        if daily:
            rain_probability = daily[0].get("precipitationProbability", {}).get("value")
    
    if rain_probability is not None:

        if rain_probability > 60:
            context["rain_prob_gt_60"] = True

        elif 30 <= rain_probability <= 60:
            context["rain_prob_30_60"] = True

        else:
            context["rain_prob_lt_30"] = True

    for alert in plot_alerts:

        notif_type = alert.get("notifTypeId")
        text = alert.get("text", "").lower()

        alert_name = ALERT_TYPE_MAP.get(notif_type)

        if alert_name:
            context[alert_name] = True
    
    detected_triggers = []

    for trigger in TRIGGER_PRIORITY:

        if context.get(trigger):
            detected_triggers.append(trigger)

    context["triggers"] = detected_triggers

    return context


def evaluate_rules(plot_alerts: List[Alert], weather_data: Dict[str, Any]) -> List[str]:
    """
    Evaluate rule table to generate advisory messages.

    Steps:
    - Build context from alerts and weather
    - Identify active triggers
    - Apply rule conditions
    - Generate messages

    Parameters
    ----------
    plot_alerts : List[Dict]
        Alerts for a plot
    weather_data : Dict
        Weather forecast

    Returns
    -------
    List[str]
        Advisory messages for the plot
    """

    context = build_context(plot_alerts, weather_data)

    print("DEBUG CONTEXT:", context)

    messages = []

    triggers = context.get("triggers", [])

    # sort triggers by priority
    triggers = sorted(
        triggers,
        key=lambda t: TRIGGER_PRIORITY.get(t, 999)
    )

    for trigger in triggers:

        trigger_rules = RULES_BY_TRIGGER.get(trigger, [])

        for rule in trigger_rules:

            if rule["trigger"] != trigger:
                continue

            condition = rule["condition"]
            condition_fn = CONDITIONS.get(condition)

            if condition_fn and condition_fn(context):
                messages.append(rule["message"])
                break

    return messages

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