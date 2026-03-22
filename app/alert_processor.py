"""
Alert processing logic.
"""

from datetime import datetime, UTC
from collections import defaultdict
from app.database import (
    insert_rejected_alert,
    get_processed_alert_ids
)
from app.config import load_rules
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

class AlertMeta(TypedDict):
    id: str
    notifTypeId: int | None
    date: str | None

class AlertPayload(TypedDict):
    alerts: List[AlertMeta]
    plotId: str
    text: str

class PlotMessage(TypedDict):
    alert: AlertPayload
    farmer: Farmer

class Rule(TypedDict):
    trigger: str
    condition: str
    message: str

    
def is_supported_alert(notif_type_id: int) -> bool:
    return notif_type_id in ALERT_TYPE_MAP


def is_duplicate_alert(alert_id: str, processed_alert_ids: set) -> bool:
    return alert_id in processed_alert_ids


def is_expired_alert(alert: Alert) -> bool:
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

def check_supported_alert(alert: Alert) -> tuple[bool, str | None]:
    if not is_supported_alert(alert.get("notifTypeId")):
        return False, "unsupported_alert_type"
    return True, None


def check_duplicate_alert(alert: Alert, processed_alert_ids: set) -> tuple[bool, str | None]:
    if is_duplicate_alert(alert["id"], processed_alert_ids):
        return False, "duplicate_alert"
    return True, None


def check_expired_alert(alert: Alert) -> tuple[bool, str | None]:
    if is_expired_alert(alert):
        return False, "expired_alert"
    return True, None


def check_sensor_validation(alert: Alert, sensors: Dict[str, Any]) -> tuple[bool, str | None]:
    if not validate_sensor_conditions(alert, sensors):
        return False, "sensor_validation_failed"
    return True, None

def validate_alert(alert: Alert, sensors: Dict[str, Any], processed_alert_ids: set) -> tuple[bool, str | None]:

    validators = [
        lambda: check_supported_alert(alert),
        lambda: check_duplicate_alert(alert, processed_alert_ids),
        lambda: check_expired_alert(alert),
        lambda: check_sensor_validation(alert, sensors),
    ]

    for validate in validators:
        is_valid, reason = validate()
        if not is_valid:
            return False, reason

    return True, None

def group_valid_alerts_by_plot(alerts: List[Alert], plot_sensor_map: Dict[str, Any], processed_alert_ids: set) -> tuple[Dict[str, List[Alert]], List[RejectedAlert]]:
    """
    Filter and validate incoming alerts.
    """
    grouped_alerts = defaultdict(list)
    rejected_alerts = []

    for alert in alerts:
        plot_id = alert.get("plotId")

        sensors = plot_sensor_map.get(plot_id, {})

        is_valid, reason = validate_alert(
            alert=alert,
            sensors=sensors,
            processed_alert_ids=processed_alert_ids
        )

        if not is_valid:
            rejected_alerts.append(build_rejected_alert(alert, reason))
            continue

        grouped_alerts[plot_id].append(alert)

    return grouped_alerts, rejected_alerts

def merge_advisory_messages(messages: List[str]) -> str:
    """
    Combine multiple advisory messages into a single string.
    """
    return "\n\n".join(messages)

def build_alert_payload(plot_id: str, plot_alerts: List[Alert], message: str) -> AlertPayload:
    return {
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
    }

def generate_plot_notification(plot_id: str, plot_alerts: List[Alert], weather_data: Dict[str, Any], farmer: Farmer, rules_by_trigger: Dict[str, List[Rule]]) -> PlotMessage | None:
    """
    Generate message for a single plot.
    """
    advisory_messages = generate_advisory_messages(plot_alerts, weather_data, rules_by_trigger)

    if not advisory_messages:
        return None

    combined_message = merge_advisory_messages(advisory_messages)

    return {
        "alert": build_alert_payload(
            plot_id=plot_id,
            plot_alerts=plot_alerts,
            message=combined_message,
        ),
        "farmer": farmer
    }

def generate_plot_notifications(grouped_alerts: Dict[str, List[Alert]], plot_weather_map: Dict[str, Any], plot_farmer_map: Dict[str, Farmer], rules_by_trigger: Dict[str, List[Rule]]) -> List[PlotMessage]:
    """
    Build notification messages for each plot using rule engine.
    """

    messages_to_send: List[PlotMessage] = []

    for plot_id, plot_alerts in grouped_alerts.items():
        weather_data = plot_weather_map.get(plot_id)
        farmer = plot_farmer_map.get(plot_id, {})

        message = generate_plot_notification(
            plot_id,
            plot_alerts,
            weather_data,
            farmer,
            rules_by_trigger
        )

        if message:
            messages_to_send.append(message)

    return messages_to_send

def persist_rejected_alerts(connection, rejected_alerts: List[RejectedAlert]) -> None:
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

def process_and_generate_notifications(alerts: List[Alert], plot_weather_map: Dict[str, Any], plot_sensor_map: Dict[str, Any], connection, plot_farmer_map: Dict[str, Farmer]) -> List[PlotMessage]:
    """
    Main orchestration function for alert processing.
    """
    processed_alert_ids = get_processed_alert_ids(connection)
    rule_table = load_rules()
    rules_by_trigger = build_rules_by_trigger(rule_table)

    grouped_alerts, rejected_alerts = group_valid_alerts_by_plot(
        alerts=alerts,
        plot_sensor_map=plot_sensor_map,
        processed_alert_ids=processed_alert_ids
    )

    persist_rejected_alerts(connection, rejected_alerts)

    messages_to_send = generate_plot_notifications(
        grouped_alerts=grouped_alerts,
        plot_weather_map=plot_weather_map,
        plot_farmer_map=plot_farmer_map,
        rules_by_trigger=rules_by_trigger
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

def build_rules_by_trigger(rule_table: List[Rule]) -> Dict[str, List[Rule]]:
    rules_by_trigger: Dict[str, List[Rule]] = {}

    for rule in rule_table:
        trigger = rule["trigger"]

        if trigger not in rules_by_trigger:
            rules_by_trigger[trigger] = []

        rules_by_trigger[trigger].append(rule)

    return rules_by_trigger

CONDITIONS = {
    "always": lambda ctx: True,
    "rain_alert": lambda ctx: ctx.get("rain_alert"),
    "rain_prob_gt_60": lambda ctx: ctx.get("rain_prob_gt_60"),
    "rain_prob_30_60": lambda ctx: ctx.get("rain_prob_30_60"),
    "rain_prob_lt_30": lambda ctx: ctx.get("rain_prob_lt_30"),
}

def validate_sensor_conditions(alert: Alert, sensors: Dict[str, Any]) -> bool:
    
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

def build_rule_evaluation_context(plot_alerts: List[Alert], weather_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build rule evaluation context from alerts and weather.
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

def does_rule_apply(rule: Rule, context: Dict[str, Any]) -> bool:
    condition = rule["condition"]
    condition_fn = CONDITIONS.get(condition)

    if not condition_fn:
        return False

    return condition_fn(context)

def select_message_for_trigger(trigger: str, context: Dict[str, Any], rules_by_trigger: Dict[str, List[Rule]]) -> str | None:

    trigger_rules = rules_by_trigger.get(trigger, [])

    for rule in trigger_rules:
        if does_rule_apply(rule, context):
            return rule["message"]

    return None

def generate_advisory_messages(plot_alerts: List[Alert], weather_data: Dict[str, Any], rules_by_trigger: Dict[str, List[Rule]]) -> List[str]:
    """
    Evaluate rule table to generate advisory messages.
    """
    context = build_rule_evaluation_context(plot_alerts, weather_data)

    triggers = sorted(
        context.get("triggers", []),
        key=lambda t: TRIGGER_PRIORITY.get(t, 999)
    )

    messages = []

    for trigger in triggers:
        message = select_message_for_trigger(trigger, context, rules_by_trigger)
        if message:
            messages.append(message)

    return messages

def is_alert_valid(alert: Alert) -> bool:
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