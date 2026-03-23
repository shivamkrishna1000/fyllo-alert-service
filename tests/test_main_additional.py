from unittest.mock import MagicMock, patch

from app.main import build_alert_context, get_plot_farmer_map, main, run_pipeline


def test_get_plot_farmer_map():
    plots = [{"plotId": "1", "farmerName": "Shivam", "farmerMobile": "9999999999"}]

    result = get_plot_farmer_map(plots)

    assert result["1"]["farmer_name"] == "Shivam"
    assert result["1"]["mobile_number"] == "919999999999"


def test_build_alert_context():
    plots = [{"plotId": "1"}]
    live_data = {
        "1": {
            "alerts": [{"id": "a1"}],
            "sensors": {"temp": 25},
        }
    }

    alerts, sensors = build_alert_context(plots, live_data)

    assert len(alerts) == 1
    assert sensors["1"] == {"temp": 25}


@patch("app.main.process_and_generate_notifications")
@patch("app.main.fetch_plot_data")
@patch("app.main.get_latest_processed_date")
def test_run_pipeline(mock_date, mock_fetch, mock_process):
    mock_date.return_value = None
    mock_fetch.return_value = ({}, {})
    mock_process.return_value = []

    client = MagicMock()
    connection = MagicMock()
    plots = []
    plot_farmer_map = {}

    run_pipeline(client, connection, plots, plot_farmer_map)

    assert mock_fetch.called
    assert mock_process.called


@patch("app.main.get_connection")
@patch("app.main.FylloClient")
def test_main_runs(mock_client, mock_conn):
    mock_conn.return_value = None
    mock_client.return_value.fetch_plots.return_value = []

    main()

    assert True
