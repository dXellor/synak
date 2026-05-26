import pytest

from syncd.sync.base import SyncContext, SyncProvider, ProviderStatus
from syncd.sync.registry import (
    DuplicateProviderError,
    ProviderRegistry,
    UnknownProviderError,
)


class MockProvider(SyncProvider):
    NAME = "mock"
    SCHEMA: dict = {}

    async def start(self, context: SyncContext) -> None: pass
    async def stop(self) -> None: pass
    async def pause(self) -> None: pass
    async def resume(self) -> None: pass
    async def trigger(self) -> None: pass
    async def status(self) -> ProviderStatus:
        return ProviderStatus(pair_id="", state="idle", last_sync=0.0, error="")


class AnotherProvider(SyncProvider):
    NAME = "another"
    SCHEMA: dict = {}

    async def start(self, context: SyncContext) -> None: pass
    async def stop(self) -> None: pass
    async def pause(self) -> None: pass
    async def resume(self) -> None: pass
    async def trigger(self) -> None: pass
    async def status(self) -> ProviderStatus:
        return ProviderStatus(pair_id="", state="idle", last_sync=0.0, error="")


def make_registry() -> ProviderRegistry:
    r = ProviderRegistry()
    r.register(MockProvider)
    return r


def test_register_and_get():
    r = make_registry()
    assert r.get("mock") is MockProvider


def test_get_unknown_raises():
    r = ProviderRegistry()
    with pytest.raises(UnknownProviderError, match="unknown"):
        r.get("unknown")


def test_register_duplicate_raises():
    r = make_registry()
    with pytest.raises(DuplicateProviderError, match="mock"):
        r.register(MockProvider)


def test_list_names_empty():
    r = ProviderRegistry()
    assert r.list_names() == []


def test_list_names_multiple():
    r = ProviderRegistry()
    r.register(MockProvider)
    r.register(AnotherProvider)
    assert set(r.list_names()) == {"mock", "another"}


def test_each_registry_is_independent():
    r1 = ProviderRegistry()
    r2 = ProviderRegistry()
    r1.register(MockProvider)
    assert r1.list_names() == ["mock"]
    assert r2.list_names() == []
