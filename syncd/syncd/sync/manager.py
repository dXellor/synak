import logging

import jsonschema

from syncd.config import PairConfig
from syncd.sync.base import SyncContext, SyncProvider, ProviderStatus
from syncd.sync.registry import registry, UnknownProviderError

logger = logging.getLogger(__name__)


class PairNotFoundError(Exception):
    pass


class ProviderConfigError(Exception):
    pass


class SyncManager:
    def __init__(self) -> None:
        self._providers: dict[str, SyncProvider] = {}

    async def start_all(self, pairs: list[PairConfig]) -> None:
        for pair in pairs:
            await self._start_pair(pair)

    async def stop_all(self) -> None:
        for pair_id, provider in list(self._providers.items()):
            try:
                await provider.stop()
            except Exception:
                logger.exception("Error stopping provider for pair %s", pair_id)
        self._providers.clear()

    async def trigger(self, pair_id: str) -> None:
        await self._get(pair_id).trigger()

    async def pause(self, pair_id: str) -> None:
        await self._get(pair_id).pause()

    async def resume(self, pair_id: str) -> None:
        await self._get(pair_id).resume()

    async def status(self, pair_id: str) -> ProviderStatus:
        return await self._get(pair_id).status()

    async def all_statuses(self) -> list[ProviderStatus]:
        return [await p.status() for p in self._providers.values()]

    def _get(self, pair_id: str) -> SyncProvider:
        if pair_id not in self._providers:
            raise PairNotFoundError(pair_id)
        return self._providers[pair_id]

    async def _start_pair(self, pair: PairConfig) -> None:
        try:
            provider_cls = registry.get(pair.mode)
        except UnknownProviderError:
            logger.error("Unknown provider mode %r for pair %r — skipping", pair.mode, pair.id)
            return

        try:
            jsonschema.validate(pair.provider, provider_cls.SCHEMA)
        except jsonschema.ValidationError as e:
            raise ProviderConfigError(
                f"Invalid provider config for pair {pair.id!r}: {e.message}"
            ) from e

        context = SyncContext(
            pair_id=pair.id,
            local=pair.local,
            direction=pair.direction,
            interval=pair.interval,
            provider_config=pair.provider,
            exclude=list(pair.exclude),
            group=pair.group,
        )
        provider = provider_cls()
        await provider.start(context)
        self._providers[pair.id] = provider
        logger.info("Started provider %r for pair %r", pair.mode, pair.id)
