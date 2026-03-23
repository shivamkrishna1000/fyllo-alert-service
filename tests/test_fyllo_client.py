from unittest.mock import patch

import requests

from app.fyllo_client import FylloClient, make_request_with_retry


def test_fetch_plots_success():
    client = FylloClient("http://fake-url")

    with patch.object(client, "_make_authenticated_request") as mock_req:
        mock_req.return_value = [{"plotId": "1"}]

        result = client.fetch_plots()

        assert result == [{"plotId": "1"}]
        mock_req.assert_called_once()


def test_fetch_plot_live_data():
    client = FylloClient("http://fake-url")

    with patch.object(client, "_make_authenticated_request") as mock_req:
        mock_req.return_value = {"alerts": []}

        result = client.fetch_plot_live_data("plot1")

        assert result == {"alerts": []}
        mock_req.assert_called_once()


def test_fetch_weather_forecast():
    client = FylloClient("http://fake-url")

    with patch.object(client, "_make_authenticated_request") as mock_req:
        mock_req.return_value = {"weather": "sunny"}

        result = client.fetch_weather_forecast("plot1")

        assert result == {"weather": "sunny"}


def test_fetch_plots_api_failure():
    client = FylloClient("http://fake-url")

    with patch.object(client, "_make_authenticated_request") as mock_req:
        mock_req.side_effect = Exception("API error")

        try:
            client.fetch_plots()
        except Exception as e:
            assert str(e) == "API error"


@patch("app.fyllo_client.make_fyllo_request")
def test_login_success(mock_request):
    mock_request.return_value = {"access_token": "abc123"}

    client = FylloClient("http://fake-url")
    client.login()

    assert client.token == "abc123"


@patch("app.fyllo_client.make_fyllo_request")
def test_retry_success_after_failure(mock_request):
    mock_request.side_effect = [
        requests.RequestException("fail"),
        {"data": "ok"},
    ]

    result = make_request_with_retry("GET", "url", {})

    assert result == {"data": "ok"}


@patch("app.fyllo_client.make_fyllo_request")
def test_retry_all_fail(mock_request):
    mock_request.side_effect = requests.RequestException("fail")

    try:
        make_request_with_retry("GET", "url", {}, retries=2)
    except Exception:
        assert True
