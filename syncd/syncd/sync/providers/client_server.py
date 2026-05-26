import asyncio
import logging
import time
from typing import Any

from syncd.sync.base import SyncContext, SyncProvider, ProviderStatus

logger = logging.getLogger(__name__)


class ClientServerProvider(SyncProvider):
    NAME = "client-server"
    SCHEMA: dict[str, Any] = {
        "type": "object",
        "properties": {
            "remote": {"type": "string", "minLength": 1},
        },
        "required": ["remote"],
        "additionalProperties": False,
    }

    def __init__(self) -> None:
        self._context: SyncContext | None = None
        self._state = "stopped"
        self._last_sync: float = 0.0
        self._error: str = ""
        self._task: asyncio.Task | None = None
        self._paused = asyncio.Event()
        self._paused.set()  # not paused initially

    async def start(self, context: SyncContext) -> None:
        self._context = context
        self._state = "idle"
        self._task = asyncio.create_task(self._sync_loop(), name=f"sync-{context.pair_id}")

    async def stop(self) -> None:
        self._state = "stopped"
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def pause(self) -> None:
        self._paused.clear()
        self._state = "paused"

    async def resume(self) -> None:
        self._paused.set()
        if self._state == "paused":
            self._state = "idle"

    async def trigger(self) -> None:
        asyncio.create_task(self._run_rsync(), name=f"trigger-{self._context.pair_id if self._context else 'unknown'}")

    async def status(self) -> ProviderStatus:
        return ProviderStatus(
            pair_id=self._context.pair_id if self._context else "",
            state=self._state,
            last_sync=self._last_sync,
            error=self._error,
        )

    async def _sync_loop(self) -> None:
        assert self._context is not None
        if self._context.interval == 0:
            await self._watch_loop()
        else:
            await self._interval_loop()

    async def _interval_loop(self) -> None:
        ctx = self._context
        assert ctx is not None
        while True:
            await self._paused.wait()
            await self._run_rsync()
            await asyncio.sleep(ctx.interval)

    async def _watch_loop(self) -> None:
        import watchfiles
        ctx = self._context
        assert ctx is not None
        try:
            async for changes in watchfiles.awatch(ctx.local):
                await self._paused.wait()
                if changes:
                    await self._run_rsync()
        except asyncio.CancelledError:
            raise

    async def _run_rsync(self) -> None:
        ctx = self._context
        assert ctx is not None
        self._state = "syncing"
        self._error = ""
        try:
            args = self._build_rsync_args(ctx)
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                self._error = stderr.decode().strip()
                self._state = "error"
                logger.error("rsync failed for pair %r: %s", ctx.pair_id, self._error)
            else:
                self._last_sync = time.time()
                self._state = "idle"
                logger.info("sync completed for pair %r", ctx.pair_id)
        except asyncio.CancelledError:
            self._state = "stopped"
            raise
        except Exception as e:
            self._error = str(e)
            self._state = "error"
            logger.exception("sync error for pair %r", ctx.pair_id)

    def _build_rsync_args(self, ctx: SyncContext) -> list[str]:
        remote: str = ctx.provider_config["remote"]
        base = ["rsync", "-az", "--delete"]
        if ctx.direction == "push":
            return base + [ctx.local + "/", remote]
        elif ctx.direction == "pull":
            return base + [remote + "/", ctx.local]
        else:
            # TODO: bidirectional rsync is not conflict-safe; this is push-only for now
            return base + [ctx.local + "/", remote]
