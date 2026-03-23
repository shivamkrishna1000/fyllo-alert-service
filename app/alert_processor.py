"""
Alert processing logic.
"""

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, Dict, List, TypedDict

from app.config import load_rules
from app.database import get_processed_alert_ids, insert_rejected_alert


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
        "alert_date": datetime.fromisoformat(alert["date"].replace("Z", "+00:00")),
    }


def check_supported_alert(alert: Alert) -> tuple[bool, str | None]:
    if not is_supported_alert(alert.get("notifTypeId")):
        return False, "unsupported_alert_type"
    return True, None


def check_duplicate_alert(
    alert: Alert, processed_alert_ids: set
) -> tuple[bool, str | None]:
    if is_duplicate_alert(alert["id"], processed_alert_ids):
        return False, "duplicate_alert"
    return True, None


def check_expired_alert(alert: Alert) -> tuple[bool, str | None]:
    if is_expired_alert(alert):
        return False, "expired_alert"
    return True, None


def check_sensor_validation(
    alert: Alert, sensors: Dict[str, Any]
) -> tuple[bool, str | None]:
    if not validate_sensor_conditions(alert, sensors):
        return False, "sensor_validation_failed"
    return True, None


def validate_alert(
    alert: Alert, sensors: Dict[str, Any], processed_alert_ids: set
) -> tuple[bool, str | None]:
    """
    Validate a single alert against all validation rules.

    Validation includes:
    - supported alert type
    - duplicate check
    - expiry check
    - sensor validation

    Parameters
    ----------
    alert : Alert
        Incoming alert object.
    sensors : Dict[str, Any]
        Sensor data for the plot.
    processed_alert_ids : set
        Set of already processed alert IDs.

    Returns
    -------
    tuple[bool, str | None]
        (is_valid, reason_if_invalid)
    """
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


def group_valid_alerts_by_plot(
    alerts: List[Alert], plot_sensor_map: Dict[str, Any], processed_alert_ids: set
) -> tuple[Dict[str, List[Alert]], List[RejectedAlert]]:
    """
    Validate alerts and group valid ones by plot.

    Invalid alerts are collected with rejection reasons.

    Parameters
    ----------
    alerts : List[Alert]
        List of incoming alerts.
    plot_sensor_map : Dict[str, Any]
        Mapping of plot_id to sensor data.
    processed_alert_ids : set
        Set of already processed alert IDs.

    Returns
    -------
    tuple[Dict[str, List[Alert]], List[RejectedAlert]]
        (grouped_valid_alerts, rejected_alerts)
    """
    grouped_alerts = defaultdict(list)
    rejected_alerts = []

    for alert in alerts:
        plot_id = alert.get("plotId")

        sensors = plot_sensor_map.get(plot_id, {})

        is_valid, reason = validate_alert(
            alert=alert, sensors=sensors, processed_alert_ids=processed_alert_ids
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


def build_alert_payload(
    plot_id: str, plot_alerts: List[Alert], message: str
) -> AlertPayload:
    return {
        "alerts": [
            {
                "id": alert["id"],
                "notifTypeId": alert.get("notifTypeId"),
                "date": alert.get("date"),
            }
            for alert in plot_alerts
        ],
        "plotId": plot_id,
        "text": message,
    }


def generate_plot_notification(
    plot_id: str,
    plot_alerts: List[Alert],
    weather_data: Dict[str, Any],
    farmer: Farmer,
    rules_by_trigger: Dict[str, List[Rule]],
) -> PlotMessage | None:
    """
    Generate advisory message for a single plot.

    Parameters
    ----------
    plot_id : str
        Plot identifier.
    plot_alerts : List[Alert]
        Valid alerts for the plot.
    weather_data : Dict[str, Any]
        Weather forecast data.
    farmer : Farmer
        Farmer details.
    rules_by_trigger : Dict[str, List[Rule]]
        Rule mapping by trigger.

    Returns
    -------
    PlotMessage | None
        Generated message or None if no advisory applicable.
    """
    advisory_messages = generate_advisory_messages(
        plot_alerts, weather_data, rules_by_trigger
    )

    if not advisory_messages:
        return None

    combined_message = merge_advisory_messages(advisory_messages)

    return {
        "alert": build_alert_payload(
            plot_id=plot_id,
            plot_alerts=plot_alerts,
            message=combined_message,
        ),
        "farmer": farmer,
    }


def generate_plot_notifications(
    grouped_alerts: Dict[str, List[Alert]],
    plot_weather_map: Dict[str, Any],
    plot_farmer_map: Dict[str, Farmer],
    rules_by_trigger: Dict[str, List[Rule]],
) -> List[PlotMessage]:
    """
    Generate advisory messages for all plots.

    Parameters
    ----------
    grouped_alerts : Dict[str, List[Alert]]
        Alerts grouped by plot.
    plot_weather_map : Dict[str, Any]
        Weather data per plot.
    plot_farmer_map : Dict[str, Farmer]
        Farmer details per plot.
    rules_by_trigger : Dict[str, List[Rule]]
        Rule mapping.

    Returns
    -------
    List[PlotMessage]
        List of messages to be sent.
    """
    messages_to_send: List[PlotMessage] = []

    for plot_id, plot_alerts in grouped_alerts.items():
        weather_data = plot_weather_map.get(plot_id)
        farmer = plot_farmer_map.get(plot_id, {})

        message = generate_plot_notification(
            plot_id, plot_alerts, weather_data, farmer, rules_by_trigger
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


def process_and_generate_notifications(
    alerts: List[Alert],
    plot_weather_map: Dict[str, Any],
    plot_sensor_map: Dict[str, Any],
    connection,
    plot_farmer_map: Dict[str, Farmer],
) -> List[PlotMessage]:
    """
    Process alerts and generate notifications.

    Pipeline:
    1. Load processed alerts
    2. Validate and group alerts
    3. Store rejected alerts
    4. Generate advisory messages

    Parameters
    ----------
    alerts : List[Alert]
        Incoming alerts.
    plot_weather_map : Dict[str, Any]
        Weather data per plot.
    plot_sensor_map : Dict[str, Any]
        Sensor data per plot.
    connection
        Database connection.
    plot_farmer_map : Dict[str, Farmer]
        Farmer details per plot.

    Returns
    -------
    List[PlotMessage]
        Messages ready for notification.
    """
    processed_alert_ids = get_processed_alert_ids(connection)
    rule_table = load_rules()
    rules_by_trigger = build_rules_by_trigger(rule_table)

    grouped_alerts, rejected_alerts = group_valid_alerts_by_plot(
        alerts=alerts,
        plot_sensor_map=plot_sensor_map,
        processed_alert_ids=processed_alert_ids,
    )

    persist_rejected_alerts(connection, rejected_alerts)

    messages_to_send = generate_plot_notifications(
        grouped_alerts=grouped_alerts,
        plot_weather_map=plot_weather_map,
        plot_farmer_map=plot_farmer_map,
        rules_by_trigger=rules_by_trigger,
    )

    return messages_to_send


ALERT_TYPE_MAP = {
    1: "irrigation",
    17: "low_soil_temp",
    18: "high_soil_temp",
    23: "rain_alert",
    24: "high_wind",
}

TRIGGER_PRIORITY = {
    "rain_alert": 1,
    "irrigation": 2,
    "nutrient": 3,
    "low_soil_temp": 4,
    "high_soil_temp": 4,
    "high_wind": 5,
}


def build_rules_by_trigger(rule_table: List[Rule]) -> Dict[str, List[Rule]]:
    """
    Organize rules by trigger type.

    Parameters
    ----------
    rule_table : List[Rule]
        List of rule definitions.

    Returns
    -------
    Dict[str, List[Rule]]
        Mapping of trigger → list of rules.
    """
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


def validate_irrigation(sensors: Dict[str, Any]) -> bool:
    moisture1 = sensors.get("moisture1")
    moisture2 = sensors.get("moisture2")

    if not moisture1 and not moisture2:
        return False

    for moisture in [moisture1, moisture2]:
        if not moisture:
            continue

        value = moisture.get("value")
        min_opt = moisture.get("minOptimalValue")

        if value is not None and min_opt is not None:
            if float(value) < float(min_opt):
                return True

    return False


def validate_low_soil_temp(sensors: Dict[str, Any]) -> bool:
    soil_temp = sensors.get("soilTemp")

    if not soil_temp:
        return False

    value = soil_temp.get("value")
    min_opt = soil_temp.get("minOptimalValue")

    if value is None or min_opt is None:
        return False

    return float(value) < float(min_opt)


def validate_high_soil_temp(sensors: Dict[str, Any]) -> bool:
    soil_temp = sensors.get("soilTemp")

    if not soil_temp:
        return False

    value = soil_temp.get("value")
    max_opt = soil_temp.get("maxOptimalValue")

    if value is None or max_opt is None:
        return False

    return float(value) > float(max_opt)


def validate_sensor_conditions(alert: Alert, sensors: Dict[str, Any]) -> bool:
    """
    Validate alert against sensor conditions.
    """

    notif_type = alert.get("notifTypeId")

    validation_map = {
        1: validate_irrigation,
        17: validate_low_soil_temp,
        18: validate_high_soil_temp,
    }

    validator = validation_map.get(notif_type)

    if not validator:
        # Rain / wind / others do not require validation
        return True

    return validator(sensors)


def build_rule_evaluation_context(
    plot_alerts: List[Alert], weather_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Build evaluation context for rule engine.

    Context includes:
    - detected alert triggers
    - rain probability categories

    Parameters
    ----------
    plot_alerts : List[Alert]
        Alerts for a plot.
    weather_data : Dict[str, Any]
        Weather forecast data.

    Returns
    -------
    Dict[str, Any]
        Context used for rule evaluation.
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
    """
    Check if a rule applies for given context.

    Parameters
    ----------
    rule : Rule
        Rule definition.
    context : Dict[str, Any]
        Evaluation context.

    Returns
    -------
    bool
        True if rule condition is satisfied.
    """
    condition = rule["condition"]
    condition_fn = CONDITIONS.get(condition)

    if not condition_fn:
        return False

    return condition_fn(context)


def select_message_for_trigger(
    trigger: str, context: Dict[str, Any], rules_by_trigger: Dict[str, List[Rule]]
) -> str | None:
    """
    Select applicable message for a trigger.

    Parameters
    ----------
    trigger : str
        Trigger name.
    context : Dict[str, Any]
        Evaluation context.
    rules_by_trigger : Dict[str, List[Rule]]
        Rule mapping.

    Returns
    -------
    str | None
        Matching message or None.
    """
    trigger_rules = rules_by_trigger.get(trigger, [])

    for rule in trigger_rules:
        if does_rule_apply(rule, context):
            return rule["message"]

    return None


def generate_advisory_messages(
    plot_alerts: List[Alert],
    weather_data: Dict[str, Any],
    rules_by_trigger: Dict[str, List[Rule]],
) -> List[str]:
    """
    Generate advisory messages based on rules and context.

    Parameters
    ----------
    plot_alerts : List[Alert]
        Alerts for a plot.
    weather_data : Dict[str, Any]
        Weather forecast data.
    rules_by_trigger : Dict[str, List[Rule]]
        Rule mapping.

    Returns
    -------
    List[str]
        List of advisory messages.
    """
    context = build_rule_evaluation_context(plot_alerts, weather_data)

    triggers = sorted(
        context.get("triggers", []), key=lambda t: TRIGGER_PRIORITY.get(t, 999)
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
        expiry_time = datetime.fromisoformat(valid_till.replace("Z", "+00:00"))
        current_time = datetime.now(UTC)
        return current_time < expiry_time
    except ValueError:
        return True
