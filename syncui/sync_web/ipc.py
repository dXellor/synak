"""Platform-specific IPC helpers (mirrors synctl/synctl/platform/ipc.py for sync httpx)."""

from __future__ import annotations

import os
import sys

import httpx

_IS_WINDOWS = sys.platform == "win32"
_WINDOWS_IPC_PORT = 24017


def default_socket_address() -> str:
    if _IS_WINDOWS:
        return f"127.0.0.1:{_WINDOWS_IPC_PORT}"
    return f"/run/user/{os.getuid()}/syncd.sock"


def make_transport(address: str) -> httpx.HTTPTransport:
    if not _IS_WINDOWS and _is_unix_path(address):
        return httpx.HTTPTransport(uds=address)
    return httpx.HTTPTransport()


def base_url(address: str) -> str:
    if not _IS_WINDOWS and _is_unix_path(address):
        return "http://syncd"
    host, port = address.rsplit(":", 1)
    return f"http://{host}:{port}"


def _is_unix_path(address: str) -> bool:
    return address.startswith("/")
