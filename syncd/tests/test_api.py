import dataclasses
import asyncio
import pytest
from aiohttp.test_utils import TestClient, TestServer
import aiohttp.web as web

from syncd.api.routes import build_routes
from syncd.config import AppConfig, DaemonConfig, PeersConfig
from syncd.sync.base import ProviderStatus


def make_config(pairs=None) -> AppConfig:
    return AppConfig(
        daemon=DaemonConfig(api_socket="/tmp/test.sock", log_level="info"),
        pairs=pairs or [],
        peers=PeersConfig(discovery="static", static=[]),
    )


class MockDaemon:
    def __init__(self):
        self.shutdown_called = False
        self.reload_called = False

    def _handle_sigterm(self):
        self.shutdown_called = True

    async def _reload_config(self):
        self.reload_called = True


class MockManager:
    def __init__(self, statuses=None):
        self._statuses: dict[str, ProviderStatus] = statuses or {}

    async def all_statuses(self) -> list[ProviderStatus]:
        return list(self._statuses.values())

    async def status(self, pair_id: str) -> ProviderStatus:
        if pair_id not in self._statuses:
            raise _NotFoundError(pair_id)
        return self._statuses[pair_id]

    async def trigger(self, pair_id: str) -> None:
        if pair_id not in self._statuses:
            raise _NotFoundError(pair_id)

    async def pause(self, pair_id: str) -> None:
        if pair_id not in self._statuses:
            raise _NotFoundError(pair_id)

    async def resume(self, pair_id: str) -> None:
        if pair_id not in self._statuses:
            raise _NotFoundError(pair_id)


class _NotFoundError(Exception):
    pass


def make_app(daemon=None, config=None, manager=None) -> web.Application:
    app = web.Application()
    app["state"] = {
        "daemon": daemon or MockDaemon(),
        "config": config or make_config(),
        "manager": manager,
    }
    app.add_routes(build_routes())
    return app


@pytest.fixture
def daemon():
    return MockDaemon()


@pytest.fixture
async def client(aiohttp_client, daemon):
    app = make_app(daemon=daemon)
    return await aiohttp_client(app)


async def test_status_returns_200(client):
    resp = await client.get("/status")
    assert resp.status == 200
    data = await resp.json()
    assert "pairs" in data
    assert "version" in data
    assert "uptime" in data


async def test_status_includes_pair_statuses(aiohttp_client):
    status = ProviderStatus(pair_id="p1", state="idle", last_sync=0.0, error="")
    manager = MockManager(statuses={"p1": status})
    app = make_app(manager=manager)
    client = await aiohttp_client(app)
    resp = await client.get("/status")
    data = await resp.json()
    assert len(data["pairs"]) == 1
    assert data["pairs"][0]["pair_id"] == "p1"


async def test_config_returns_serialized_config(client):
    resp = await client.get("/config")
    assert resp.status == 200
    data = await resp.json()
    assert "daemon" in data
    assert "pairs" in data
    assert "peers" in data


async def test_config_reload_returns_200(client):
    resp = await client.post("/config/reload")
    assert resp.status == 200


async def test_peers_returns_peers_config(client):
    resp = await client.get("/peers")
    assert resp.status == 200
    data = await resp.json()
    assert "discovery" in data


async def test_pairs_returns_list(aiohttp_client):
    from syncd.config import PairConfig
    pair = PairConfig(id="x", mode="p2p", local="/tmp/x",
                      direction="push", interval=0, provider={})
    config = make_config(pairs=[pair])
    app = make_app(config=config)
    client = await aiohttp_client(app)
    resp = await client.get("/pairs")
    assert resp.status == 200
    data = await resp.json()
    assert len(data) == 1
    assert data[0]["id"] == "x"


async def test_pair_sync_unknown_returns_404(aiohttp_client):
    manager = MockManager()
    app = make_app(manager=manager)
    client = await aiohttp_client(app)
    resp = await client.post("/pairs/nonexistent/sync")
    assert resp.status == 404
    data = await resp.json()
    assert "error" in data


async def test_pair_pause_unknown_returns_404(aiohttp_client):
    manager = MockManager()
    app = make_app(manager=manager)
    client = await aiohttp_client(app)
    resp = await client.post("/pairs/nonexistent/pause")
    assert resp.status == 404


async def test_pair_resume_unknown_returns_404(aiohttp_client):
    manager = MockManager()
    app = make_app(manager=manager)
    client = await aiohttp_client(app)
    resp = await client.post("/pairs/nonexistent/resume")
    assert resp.status == 404


async def test_pair_sync_known_returns_200(aiohttp_client):
    status = ProviderStatus(pair_id="p1", state="idle", last_sync=0.0, error="")
    manager = MockManager(statuses={"p1": status})
    app = make_app(manager=manager)
    client = await aiohttp_client(app)
    resp = await client.post("/pairs/p1/sync")
    assert resp.status == 200


async def test_shutdown_calls_sigterm_handler(aiohttp_client):
    daemon = MockDaemon()
    app = make_app(daemon=daemon)
    client = await aiohttp_client(app)
    resp = await client.post("/shutdown")
    assert resp.status == 200
    assert daemon.shutdown_called


async def test_no_manager_returns_503_for_pair_action(aiohttp_client):
    app = make_app(manager=None)
    client = await aiohttp_client(app)
    resp = await client.post("/pairs/any/sync")
    assert resp.status == 503
