from typing import Any

from syncd.sync.base import SyncContext, SyncProvider, ProviderStatus


class P2PProvider(SyncProvider):
    NAME = "p2p"
    SCHEMA: dict[str, Any] = {
        "type": "object",
        "properties": {
            "peers": {
                "type": "array",
                "items": {"type": "string"},
            }
        },
        "required": ["peers"],
        "additionalProperties": False,
    }

    def __init__(self) -> None:
        self._context: SyncContext | None = None
        self._state = "stopped"

    async def start(self, context: SyncContext) -> None:
        self._context = context
        self._state = "idle"

    async def stop(self) -> None:
        self._state = "stopped"

    async def pause(self) -> None:
        self._state = "paused"

    async def resume(self) -> None:
        self._state = "idle"

    async def trigger(self) -> None:
        pass

    async def status(self) -> ProviderStatus:
        pair_id = self._context.pair_id if self._context else ""
        return ProviderStatus(
            pair_id=pair_id,
            state=self._state,
            last_sync=0.0,
            error="",
        )
