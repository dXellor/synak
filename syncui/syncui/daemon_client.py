"""Synchronous httpx client for the syncd daemon Unix socket API."""

from __future__ import annotations

from typing import Any

import httpx

from syncui.ipc import default_socket_address, make_transport, base_url


class DaemonNotRunningError(Exception):
    pass


class DaemonError(Exception):
    def __init__(self, message: str, status: int) -> None:
        super().__init__(message)
        self.status = status


class DaemonClient:
    def __init__(self, socket_path: str | None = None) -> None:
        address = socket_path or default_socket_address()
        self._transport = make_transport(address)
        self._base_url = base_url(address)
        self._address = address

    def get(self, path: str) -> Any:
        try:
            with httpx.Client(transport=self._transport, base_url=self._base_url) as client:
                resp = client.get(path)
                _raise_for_status(resp)
                return resp.json()
        except httpx.ConnectError as e:
            raise DaemonNotRunningError(
                f"Cannot connect to syncd at {self._address}"
            ) from e

    def post(self, path: str, body: dict | None = None) -> Any:
        try:
            with httpx.Client(transport=self._transport, base_url=self._base_url) as client:
                resp = client.post(path, json=body or {})
                _raise_for_status(resp)
                return resp.json()
        except httpx.ConnectError as e:
            raise DaemonNotRunningError(
                f"Cannot connect to syncd at {self._address}"
            ) from e


def _raise_for_status(resp: httpx.Response) -> None:
    if resp.is_success:
        return
    try:
        message = resp.json().get("error", resp.text)
    except Exception:
        message = resp.text
    raise DaemonError(message, status=resp.status_code)
