"""Generic subprocess sync provider — delegates all work to an external binary via stdin/stdout JSON IPC."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from asyncio.subprocess import PIPE
from typing import Any, ClassVar

from syncd.sync.base import SyncContext, SyncProvider, ProviderStatus

logger = logging.getLogger(__name__)


class SubprocessProvider(SyncProvider):
    """
    Wraps any external binary that speaks the stdin/stdout newline-delimited JSON IPC protocol.

    Commands sent to binary stdin (one JSON line each):
        {"cmd": "start",   "context": <SyncContext dict>}
        {"cmd": "stop"}
        {"cmd": "pause"}
        {"cmd": "resume"}
        {"cmd": "trigger"}
        {"cmd": "status"}

    Each command expects exactly one response line:
        {"ok": true}
        {"ok": true, "data": <ProviderStatus dict>}   # status only
        {"ok": false, "error": "<message>"}

    Binary stderr is forwarded to the Python logger at DEBUG level.
    """

    _BINARY_KEY: ClassVar[str] = "binary"

    def __init__(self) -> None:
        self._proc: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._pair_id: str = ""

    async def _send(self, cmd: dict[str, Any]) -> dict[str, Any]:
        async with self._lock:
            if self._proc is None:
                raise RuntimeError(f"{self.NAME!r} subprocess is not running")
            payload = (json.dumps(cmd) + "\n").encode()
            self._proc.stdin.write(payload)
            await self._proc.stdin.drain()
            line = await self._proc.stdout.readline()
            if not line:
                raise RuntimeError(f"{self.NAME!r} subprocess exited unexpectedly")
            return json.loads(line)

    async def start(self, context: SyncContext) -> None:
        self._pair_id = context.pair_id
        binary = context.provider_config[self._BINARY_KEY]
        self._proc = await asyncio.create_subprocess_exec(
            binary, stdin=PIPE, stdout=PIPE, stderr=PIPE
        )
        self._stderr_task = asyncio.create_task(self._drain_stderr())
        resp = await self._send({"cmd": "start", "context": dataclasses.asdict(context)})
        if not resp.get("ok"):
            raise RuntimeError(resp.get("error", "subprocess start failed"))

    async def stop(self) -> None:
        if self._proc is None:
            return
        try:
            await self._send({"cmd": "stop"})
            await self._proc.wait()
        except Exception:
            self._proc.kill()
        finally:
            if self._stderr_task:
                self._stderr_task.cancel()
                try:
                    await self._stderr_task
                except asyncio.CancelledError:
                    pass
            self._proc = None

    async def pause(self) -> None:
        await self._send({"cmd": "pause"})

    async def resume(self) -> None:
        await self._send({"cmd": "resume"})

    async def trigger(self) -> None:
        await self._send({"cmd": "trigger"})

    async def status(self) -> ProviderStatus:
        resp = await self._send({"cmd": "status"})
        data = resp.get("data") or {}
        return ProviderStatus(
            pair_id=data.get("pair_id", self._pair_id),
            state=data.get("state", "unknown"),
            last_sync=data.get("last_sync", 0.0),
            error=data.get("error", ""),
            extra=data.get("extra", {}),
        )

    async def _drain_stderr(self) -> None:
        assert self._proc is not None and self._proc.stderr is not None
        while True:
            line = await self._proc.stderr.readline()
            if not line:
                break
            logger.debug("[%s] %s", self.NAME, line.decode().rstrip())
