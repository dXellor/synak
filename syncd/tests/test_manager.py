import pytest

from syncd.config import PairConfig
from syncd.sync.base import SyncContext, SyncProvider, ProviderStatus
from syncd.sync.manager import PairNotFoundError, ProviderConfigError, SyncManager
from syncd.sync.registry import ProviderRegistry


# --- helpers ---

def make_pair(
    id="p1",
    mode="mock",
    local="/tmp/x",
    direction="push",
    interval=0,
    provider=None,
) -> PairConfig:
    return PairConfig(
        id=id,
        mode=mode,
        local=local,
        direction=direction,
        interval=interval,
        provider=provider or {},
    )


class MockProvider(SyncProvider):
    NAME = "mock"
    SCHEMA = {"type": "object", "additionalProperties": True}

    def __init__(self) -> None:
        self._context: SyncContext | None = None
        self._state = "stopped"
        self.started = False
        self.stopped = False
        self.paused = False
        self.resumed = False
        self.triggered = False

    async def start(self, context: SyncContext) -> None:
        self._context = context
        self._state = "idle"
        self.started = True

    async def stop(self) -> None:
        self._state = "stopped"
        self.stopped = True

    async def pause(self) -> None:
        self._state = "paused"
        self.paused = True

    async def resume(self) -> None:
        self._state = "idle"
        self.resumed = True

    async def trigger(self) -> None:
        self.triggered = True

    async def status(self) -> ProviderStatus:
        pair_id = self._context.pair_id if self._context else ""
        return ProviderStatus(pair_id=pair_id, state=self._state, last_sync=0.0, error="")


class StrictSchemaProvider(SyncProvider):
    NAME = "strict"
    SCHEMA = {
        "type": "object",
        "properties": {"key": {"type": "string"}},
        "required": ["key"],
        "additionalProperties": False,
    }

    async def start(self, context: SyncContext) -> None: pass
    async def stop(self) -> None: pass
    async def pause(self) -> None: pass
    async def resume(self) -> None: pass
    async def trigger(self) -> None: pass
    async def status(self) -> ProviderStatus:
        return ProviderStatus(pair_id="", state="idle", last_sync=0.0, error="")


def make_manager_with_mock() -> tuple[SyncManager, ProviderRegistry]:
    reg = ProviderRegistry()
    reg.register(MockProvider)

    import syncd.sync.manager as mgr_module
    original = mgr_module.registry

    # Patch the module-level registry used by SyncManager
    mgr_module.registry = reg
    manager = SyncManager()

    return manager, reg, original, mgr_module


# --- tests ---

async def test_start_all_starts_provider():
    import syncd.sync.manager as mgr_module
    original = mgr_module.registry
    try:
        reg = ProviderRegistry()
        reg.register(MockProvider)
        mgr_module.registry = reg
        manager = SyncManager()
        await manager.start_all([make_pair()])
        status = await manager.status("p1")
        assert status.state == "idle"
    finally:
        mgr_module.registry = original


async def test_trigger_delegates():
    import syncd.sync.manager as mgr_module
    original = mgr_module.registry
    try:
        reg = ProviderRegistry()
        reg.register(MockProvider)
        mgr_module.registry = reg
        manager = SyncManager()
        await manager.start_all([make_pair()])
        await manager.trigger("p1")
        provider = manager._providers["p1"]
        assert provider.triggered
    finally:
        mgr_module.registry = original


async def test_pause_delegates():
    import syncd.sync.manager as mgr_module
    original = mgr_module.registry
    try:
        reg = ProviderRegistry()
        reg.register(MockProvider)
        mgr_module.registry = reg
        manager = SyncManager()
        await manager.start_all([make_pair()])
        await manager.pause("p1")
        assert manager._providers["p1"].paused
    finally:
        mgr_module.registry = original


async def test_resume_delegates():
    import syncd.sync.manager as mgr_module
    original = mgr_module.registry
    try:
        reg = ProviderRegistry()
        reg.register(MockProvider)
        mgr_module.registry = reg
        manager = SyncManager()
        await manager.start_all([make_pair()])
        await manager.resume("p1")
        assert manager._providers["p1"].resumed
    finally:
        mgr_module.registry = original


async def test_trigger_unknown_pair_raises():
    manager = SyncManager()
    with pytest.raises(PairNotFoundError):
        await manager.trigger("nonexistent")


async def test_unknown_mode_logs_and_skips(caplog):
    import logging
    manager = SyncManager()
    pair = make_pair(mode="does-not-exist")
    with caplog.at_level(logging.ERROR, logger="syncd.sync.manager"):
        await manager.start_all([pair])
    assert "Unknown provider mode" in caplog.text
    assert len(manager._providers) == 0


async def test_invalid_provider_config_raises():
    import syncd.sync.manager as mgr_module
    original = mgr_module.registry
    try:
        reg = ProviderRegistry()
        reg.register(StrictSchemaProvider)
        mgr_module.registry = reg
        manager = SyncManager()
        pair = make_pair(mode="strict", provider={"wrong_key": "val"})
        with pytest.raises(ProviderConfigError):
            await manager.start_all([pair])
    finally:
        mgr_module.registry = original


async def test_stop_all_stops_all_providers():
    import syncd.sync.manager as mgr_module
    original = mgr_module.registry
    try:
        reg = ProviderRegistry()
        reg.register(MockProvider)
        mgr_module.registry = reg
        manager = SyncManager()
        await manager.start_all([make_pair("p1"), make_pair("p2")])
        await manager.stop_all()
        assert len(manager._providers) == 0
    finally:
        mgr_module.registry = original


async def test_all_statuses_returns_all():
    import syncd.sync.manager as mgr_module
    original = mgr_module.registry
    try:
        reg = ProviderRegistry()
        reg.register(MockProvider)
        mgr_module.registry = reg
        manager = SyncManager()
        await manager.start_all([make_pair("p1"), make_pair("p2")])
        statuses = await manager.all_statuses()
        assert len(statuses) == 2
    finally:
        mgr_module.registry = original


async def test_provider_registration():
    import syncd.sync.providers  # noqa: F401 — ensure side-effect runs
    from syncd.sync.registry import registry
    assert "p2p" in registry.list_names()
    assert "client-server" in registry.list_names()
