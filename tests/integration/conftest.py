import os
import subprocess
import sys
import time

import httpx
import pytest


def _wait_for_socket(path: str, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(0.05)
    return False


@pytest.fixture(scope="module")
def daemon_socket(tmp_path_factory):
    config_dir = tmp_path_factory.mktemp("syncd_integration")
    socket_path = str(config_dir / "syncd.sock")
    config_path = config_dir / "config.toml"
    config_path.write_text(
        f'[daemon]\napi_socket = "{socket_path}"\nlog_level = "debug"\n'
    )

    proc = subprocess.Popen(
        [sys.executable, "-m", "syncd.main", "-c", str(config_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if not _wait_for_socket(socket_path):
        proc.terminate()
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        pytest.fail(f"syncd did not start within 5s. stderr:\n{stderr}")

    yield socket_path

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture(scope="module")
def http_client(daemon_socket):
    transport = httpx.HTTPTransport(uds=daemon_socket)
    with httpx.Client(transport=transport, base_url="http://syncd", timeout=5.0) as client:
        yield client
