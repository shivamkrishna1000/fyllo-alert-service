import requests
import logging
from typing import Any, Dict, List
from app.config import get_farm_user_id, get_fyllo_password
import os

TOKEN_FILE = "fyllo_token.txt"

def make_fyllo_request(method: str, url: str, headers: dict, json_data: dict | None = None, timeout: int = 10):
    """
    Generic wrapper for Fyllo API requests.
    """
    if method == "GET":
        response = requests.get(url, headers=headers, timeout=timeout)
    elif method == "POST":
        response = requests.post(url, headers=headers, json=json_data, timeout=timeout)
    else:
        raise ValueError(f"Unsupported method: {method}")

    if response.status_code != 200:
        raise Exception(f"Fyllo API error: {response.status_code} - {response.text}")

    return response.json()

def make_request_with_retry(method: str, url: str, headers: dict, json_data: dict | None = None, retries: int = 3):
    """
    Wrapper with retry logic for Fyllo API.
    """
    for attempt in range(retries):
        try:
            data = make_fyllo_request(
                method=method,
                url=url,
                headers=headers,
                json_data=json_data
            )

            return data

        except requests.RequestException as exc:
            logging.warning(
                "Attempt %d/%d failed: %s",
                attempt + 1,
                retries,
                exc,
            )

            if attempt == retries - 1:
                logging.error("All retries failed.")
                raise Exception("Fyllo API failed after retries")

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

        data = make_fyllo_request(
            method="POST",
            url=url,
            headers={},
            json_data=payload
        )

        if "access_token" not in data:
            raise Exception(f"Login failed: {data}")

        self.token = data["access_token"]

        with open(TOKEN_FILE, "w") as f:
            f.write(self.token)

        logging.info("Fyllo login successful and token saved")


    def _get_headers(self) -> Dict[str, str]:

        if not self.token:
            self.login()

        return {"Authorization": f"Bearer {self.token}"}
    
    def _make_authenticated_request(self, method: str, url: str) -> Dict[str, Any]:

        headers = self._get_headers()

        try:
            return make_request_with_retry(method, url, headers)
        except Exception as e:
            if "401" in str(e):
                logging.info("Token expired. Re-authenticating.")
                self.login()
                headers = self._get_headers()
                return make_request_with_retry(method, url, headers)
            raise
                
    def fetch_plot_live_data(self, plot_id: str, notification_last_seen: str | None = None) -> Dict[str, Any]:

        url = f"{self.base_url}/plots/{plot_id}/live-data"

        return self._make_authenticated_request("GET", url)
                
    def fetch_plots(self) -> List[Dict[str, Any]]:

        url = f"{self.base_url}/plots"

        return self._make_authenticated_request("GET", url)
                
    def fetch_weather_forecast(self, plot_id: str) -> Dict[str, Any]:

        url = f"{self.base_url}/plots/{plot_id}/weather-forecast"
    
        return self._make_authenticated_request("GET", url)
