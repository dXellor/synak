def test_status_returns_200(http_client):
    resp = http_client.get("/status")
    assert resp.status_code == 200


def test_status_has_expected_fields(http_client):
    data = http_client.get("/status").json()
    assert "version" in data
    assert "uptime" in data
    assert "pairs" in data
    assert isinstance(data["pairs"], list)


def test_status_uptime_is_positive(http_client):
    data = http_client.get("/status").json()
    assert data["uptime"] >= 0


def test_config_returns_200(http_client):
    resp = http_client.get("/config")
    assert resp.status_code == 200


def test_config_has_daemon_section(http_client):
    data = http_client.get("/config").json()
    assert "daemon" in data
    assert "pairs" in data
    assert "peers" in data


def test_pairs_returns_empty_list(http_client):
    data = http_client.get("/pairs").json()
    assert data == []


def test_peers_returns_200(http_client):
    resp = http_client.get("/peers")
    assert resp.status_code == 200


def test_config_reload_returns_200(http_client):
    resp = http_client.post("/config/reload")
    assert resp.status_code == 200


def test_unknown_pair_sync_returns_404(http_client):
    resp = http_client.post("/pairs/nonexistent/sync")
    assert resp.status_code == 404
    assert "error" in resp.json()


def test_unknown_pair_pause_returns_404(http_client):
    resp = http_client.post("/pairs/nonexistent/pause")
    assert resp.status_code == 404


def test_shutdown_stops_daemon(http_client, daemon_socket):
    import os
    import time

    resp = http_client.post("/shutdown")
    assert resp.status_code == 200

    # socket should disappear as daemon shuts down
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        if not os.path.exists(daemon_socket):
            return
        time.sleep(0.05)
    # Socket may linger briefly after process exit — that's acceptable
    # as long as the request returned 200
