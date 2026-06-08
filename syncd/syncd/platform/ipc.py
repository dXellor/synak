"""Platform-specific IPC: address derivation and aiohttp site factory."""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING

import aiohttp.web as web

if TYPE_CHECKING:
    pass

_IS_WINDOWS = sys.platform == "win32"
_WINDOWS_IPC_PORT = 24017


def default_socket_address() -> str:
    """Return the platform-appropriate default daemon listen address."""
    if _IS_WINDOWS:
        return f"127.0.0.1:{_WINDOWS_IPC_PORT}"
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
    """Convert a "host:port" string to (host, port)."""
    host, port_str = address.rsplit(":", 1)
    return host, int(port_str)


def default_log_path(address: str) -> str:
    """Derive a log file path from the daemon listen address.

    Unix socket path → same path with .sock replaced by .log.
    TCP address      → <tempdir>/syncd.log  (colons are invalid in Windows filenames).
    """
    if is_unix_socket_address(address):
        return address.removesuffix(".sock") + ".log"
    import tempfile
    return os.path.join(tempfile.gettempdir(), "syncd.log")


