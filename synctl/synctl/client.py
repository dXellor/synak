import os
from typing import Any

import httpx

DEFAULT_SOCKET = "/run/user/{uid}/syncd.sock"


class DaemonNotRunningError(Exception):
    pass


class DaemonClient:
    def __init__(self, socket_path: str | None = None) -> None:
        self._socket = socket_path or DEFAULT_SOCKET.format(uid=os.getuid())
        self._transport = httpx.AsyncHTTPTransport(uds=self._socket)

    async def get(self, path: str) -> Any:
        try:
            async with httpx.AsyncClient(
                transport=self._transport, base_url="http://syncd"
            ) as client:
                resp = await client.get(path)
                resp.raise_for_status()
                return resp.json()
        except httpx.ConnectError as e:
            raise DaemonNotRunningError(
                f"Cannot connect to syncd at {self._socket}"
            ) from e

    async def post(self, path: str, body: dict | None = None) -> Any:
        try:
            async with httpx.AsyncClient(
                transport=self._transport, base_url="http://syncd"
            ) as client:
                resp = await client.post(path, json=body or {})
                resp.raise_for_status()
                return resp.json()
        except httpx.ConnectError as e:
            raise DaemonNotRunningError(
                f"Cannot connect to syncd at {self._socket}"
            ) from e
