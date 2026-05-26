import json
from unittest.mock import AsyncMock, patch

import pytest
from click.testing import CliRunner

from synctl.main import cli


STATUS_RESPONSE = {
    "version": "0.1.0",
    "uptime": 42.0,
    "pairs": [
        {"pair_id": "photos", "state": "idle", "error": ""},
    ],
}

PAIRS_RESPONSE = [
    {"id": "photos", "mode": "p2p", "direction": "push",
     "status": {"state": "idle", "error": ""}},
]

CONFIG_RESPONSE = {
    "daemon": {"api_socket": "/tmp/test.sock", "log_level": "info"},
    "pairs": [],
    "peers": {"discovery": "static", "static": []},
}


def run(args, *, get_return=None, post_return=None):
    runner = CliRunner()
    with patch("synctl.client.DaemonClient.get", new_callable=AsyncMock) as mock_get, \
         patch("synctl.client.DaemonClient.post", new_callable=AsyncMock) as mock_post:
        if get_return is not None:
            mock_get.return_value = get_return
        if post_return is not None:
            mock_post.return_value = post_return
        result = runner.invoke(cli, args, catch_exceptions=False)
    return result, mock_get, mock_post


# --- status ---

def test_status_pretty_output():
    result, mock_get, _ = run(["status"], get_return=STATUS_RESPONSE)
    assert result.exit_code == 0
    assert "photos" in result.output
    assert "idle" in result.output
    mock_get.assert_called_once_with("/status")


def test_status_json_output():
    result, _, _ = run(["--json", "status"], get_return=STATUS_RESPONSE)
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["version"] == "0.1.0"
    assert data["pairs"][0]["pair_id"] == "photos"


def test_status_shows_uptime():
    result, _, _ = run(["status"], get_return=STATUS_RESPONSE)
    assert "42" in result.output


# --- sync list ---

def test_sync_list_shows_pairs():
    result, mock_get, _ = run(["sync", "list"], get_return=PAIRS_RESPONSE)
    assert result.exit_code == 0
    assert "photos" in result.output
    mock_get.assert_called_once_with("/pairs")


def test_sync_list_json():
    result, _, _ = run(["--json", "sync", "list"], get_return=PAIRS_RESPONSE)
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data[0]["id"] == "photos"


def test_sync_list_no_pairs():
    result, _, _ = run(["sync", "list"], get_return=[])
    assert result.exit_code == 0
    assert "no pairs" in result.output


# --- sync trigger ---

def test_sync_trigger_calls_correct_endpoint():
    result, _, mock_post = run(["sync", "trigger", "work-docs"], post_return={})
    assert result.exit_code == 0
    assert "work-docs" in result.output
    mock_post.assert_called_once_with("/pairs/work-docs/sync")


# --- sync pause / resume ---

def test_sync_pause_calls_correct_endpoint():
    result, _, mock_post = run(["sync", "pause", "photos"], post_return={})
    assert result.exit_code == 0
    mock_post.assert_called_once_with("/pairs/photos/pause")


def test_sync_resume_calls_correct_endpoint():
    result, _, mock_post = run(["sync", "resume", "photos"], post_return={})
    assert result.exit_code == 0
    mock_post.assert_called_once_with("/pairs/photos/resume")


# --- config show ---

def test_config_show_outputs_json():
    result, mock_get, _ = run(["config", "show"], get_return=CONFIG_RESPONSE)
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "daemon" in data
    mock_get.assert_called_once_with("/config")


# --- config reload ---

def test_config_reload_calls_endpoint():
    result, _, mock_post = run(["config", "reload"], post_return={})
    assert result.exit_code == 0
    assert "reload" in result.output
    mock_post.assert_called_once_with("/config/reload")


# --- socket passthrough ---

def test_socket_option_passed_to_client():
    runner = CliRunner()
    captured = {}

    def fake_init(self, socket_path=None):
        captured["socket"] = socket_path
        self._socket = socket_path or "default"
        import httpx
        self._transport = httpx.AsyncHTTPTransport(uds="/tmp/fake.sock")

    with patch("synctl.client.DaemonClient.__init__", fake_init), \
         patch("synctl.client.DaemonClient.get", new_callable=AsyncMock,
               return_value=STATUS_RESPONSE):
        runner.invoke(cli, ["--socket", "/custom/path.sock", "status"],
                      catch_exceptions=False)

    assert captured["socket"] == "/custom/path.sock"
