from typing import Any

import httpx

from synctl.platform.ipc import default_socket_address, make_transport, base_url


class DaemonNotRunningError(Exception):
    pass


class DaemonError(Exception):
    pass


class DaemonClient:
    def __init__(self, socket_path: str | None = None) -> None:
        self._address = socket_path or default_socket_address()
        self._transport = make_transport(self._address)
        self._base_url = base_url(self._address)

    async def get(self, path: str) -> Any:
        try:
            async with httpx.AsyncClient(
                transport=self._transport, base_url=self._base_url
            ) as client:
                resp = await client.get(path)
                _raise_for_status(resp)
                return resp.json()
        except httpx.ConnectError as e:
            raise DaemonNotRunningError(
                f"Cannot connect to syncd at {self._address}"
            ) from e

    async def post(self, path: str, body: dict | None = None) -> Any:
        try:
            async with httpx.AsyncClient(
                transport=self._transport, base_url=self._base_url
            ) as client:
                resp = await client.post(path, json=body or {})
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
    raise DaemonError(f"{resp.status_code}: {message}")
