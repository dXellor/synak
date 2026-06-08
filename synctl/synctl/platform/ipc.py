"""Platform-specific IPC: address derivation and httpx transport factory."""

from __future__ import annotations

import hashlib
import os
import sys

import httpx

_IS_WINDOWS = sys.platform == "win32"


def default_socket_address() -> str:
    """Return the platform-appropriate default daemon address."""
    if _IS_WINDOWS:
        username = os.environ.get("USERNAME", "syncd")
        return f"127.0.0.1:{_port_from_string(username)}"
    return f"/run/user/{os.getuid()}/syncd.sock"


def make_transport(address: str) -> httpx.AsyncHTTPTransport:
    """Return an httpx transport for the given daemon address."""
    if not _IS_WINDOWS and _is_unix_path(address):
        return httpx.AsyncHTTPTransport(uds=address)
    return httpx.AsyncHTTPTransport()


def base_url(address: str) -> str:
    """Return the base_url string for httpx.AsyncClient."""
    if not _IS_WINDOWS and _is_unix_path(address):
        return "http://syncd"
    host, port = _parse_tcp(address)
    return f"http://{host}:{port}"


def _is_unix_path(address: str) -> bool:
    return address.startswith("/")


def _parse_tcp(address: str) -> tuple[str, int]:
    if ":" in address:
        host, port_str = address.rsplit(":", 1)
        return host, int(port_str)
    return "127.0.0.1", _port_from_string(address)


def _port_from_string(s: str) -> int:
    return 49152 + (int(hashlib.sha256(s.encode()).hexdigest(), 16) % 16383)
