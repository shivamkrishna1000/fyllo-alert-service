import requests
import logging
from typing import Any, Dict, List
from app.config import get_farm_user_id, get_fyllo_password
import json
import os

TOKEN_FILE = "fyllo_token.txt"

class FylloClient:

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.token: str | None = None

        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                self.token = f.read().strip()

    def login(self) -> None:

        logging.info("Logging in to Fyllo API")

        url = f"{self.base_url}/farm-users/login"

        payload = {
            "farmUserId": get_farm_user_id(),
            "otp": get_fyllo_password()
        }

        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()

        data = response.json()

        self.token = data["access_token"]

        with open(TOKEN_FILE, "w") as f:
            f.write(self.token)

        logging.info("Fyllo login successful and token saved")


    def _get_headers(self) -> Dict[str, str]:
        if not self.token:
            self.login()

        return {"Authorization": f"Bearer {self.token}"}
    
                
    def fetch_plot_live_data(self, plot_id: str, notification_last_seen: str | None = None) -> Dict[str, Any]:

        url = f"{self.base_url}/plots/{plot_id}/live-data"

        for attempt in range(3):
            try:
                params = {}
                if notification_last_seen:
                    params["notificationLastSeen"] = notification_last_seen

                response = requests.get(
                    url,
                    headers=self._get_headers(),
                    params=params,
                    timeout=10,
                )

                if response.status_code == 401:
                    logging.info("Token expired. Re-authenticating.")
                    self.login()

                    response = requests.get(
                        url,
                        headers=self._get_headers(),
                        params=params,
                        timeout=10,
                    )

                response.raise_for_status()

                return response.json()

            except requests.RequestException as exc:

                logging.warning(
                    "Attempt %d/3 failed: %s",
                    attempt + 1,
                    exc,
                )

                if attempt == 2:
                    logging.error("All retries failed.")
                    return {}
                
    def fetch_plots(self) -> List[Dict[str, Any]]:

        url = f"{self.base_url}/plots"

        for attempt in range(3):
            try:

                response = requests.get(
                    url,
                    headers=self._get_headers(),
                    timeout=10,
                )

                if response.status_code == 401:
                    logging.info("Token expired. Re-authenticating.")
                    self.login()

                    response = requests.get(
                        url,
                        headers=self._get_headers(),
                        timeout=10,
                    )

                response.raise_for_status()

                return response.json()

            except requests.RequestException as exc:

                logging.warning(
                    "Attempt %d/3 failed: %s",
                    attempt + 1,
                    exc,
                )

                if attempt == 2:
                    logging.error("All retries failed.")
                    return []
