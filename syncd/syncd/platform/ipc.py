"""Platform-specific IPC: address derivation and aiohttp site factory."""

from __future__ import annotations

import hashlib
import os
import sys
from typing import TYPE_CHECKING

import aiohttp.web as web

if TYPE_CHECKING:
    pass

_IS_WINDOWS = sys.platform == "win32"


def default_socket_address() -> str:
    """Return the platform-appropriate default daemon listen address."""
    if _IS_WINDOWS:
        username = os.environ.get("USERNAME", "syncd")
        port = _port_from_string(username)
        return f"127.0.0.1:{port}"
    return f"/run/user/{os.getuid()}/syncd.sock"


def is_unix_socket_address(address: str) -> bool:
    """True if address is a filesystem path (Unix domain socket)."""
    return address.startswith("/")


async def make_site(runner: web.AppRunner, address: str) -> web.BaseSite:
    """Create the aiohttp Site appropriate for this platform and address."""
    if not _IS_WINDOWS and is_unix_socket_address(address):
        return web.UnixSite(runner, address)
    host, port = _parse_tcp(address)
    return web.TCPSite(runner, host, port)


def _parse_tcp(address: str) -> tuple[str, int]:
    """Convert an address string to (host, port).

    Accepts "host:port" directly, or derives a stable port from a Unix-style
    path string so that Windows can use the same config file as Linux.
    """
    if ":" in address and not address.startswith("/"):
        host, port_str = address.rsplit(":", 1)
        return host, int(port_str)
    return "127.0.0.1", _port_from_string(address)


def _port_from_string(s: str) -> int:
    """Derive a stable port in 49152-65535 from an arbitrary string."""
    return 49152 + (int(hashlib.sha256(s.encode()).hexdigest(), 16) % 16383)
