"""
Client for fetching alerts from Fyllo.
"""

import logging
import requests
from typing import Any, Dict, List
from app.config import get_farm_user_id
import json


def fetch_recent_alerts(
    base_url: str,
    token: str,
    since: str | None = None,
    limit: int = 100,
    skip: int = 0,
) -> List[Dict[str, Any]]:
    """
    Fetch recent alerts from Fyllo.
    """
    url = f"{base_url}/app-notifs"

    farm_user_id = get_farm_user_id()

    where_clause = {"farmUserId": farm_user_id}

    if since:
        where_clause["date"] = {"gte": since}

    filter_query = {
        "where": where_clause,
        "order": ["date ASC"],
        "limit": limit,
        "skip": skip,
    }

    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(3):
        try:
            response = requests.get(
                url,
                headers=headers,
                params={"filter": json.dumps(filter_query)},
                timeout=10,
            )
            response.raise_for_status()
            return response.json()

        except requests.RequestException as exc:
            if attempt == 2:
                logging.error("Failed after retries: %s", exc)
                return []