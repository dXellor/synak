"""Tests for the new protocol-based ClientServerProvider."""

import asyncio
import os
import pytest

from syncd.sync.base import SyncContext
from syncd.sync.providers.client_server import ClientServerProvider


def make_context(tmp_path, mode="client", port=0, interval=60) -> SyncContext:
    return SyncContext(
        pair_id="test-pair",
        local=str(tmp_path),
        direction="bidirectional",
        interval=interval,
        provider_config={
            "mode": mode,
            "host": "127.0.0.1",
            "port": port,
        },
    )


# --- lifecycle ---

async def test_client_start_creates_task(tmp_path):
    p = ClientServerProvider()
    ctx = make_context(tmp_path, mode="client", port=19876)
    await p.start(ctx)
    assert p._task is not None
    assert p._state == "idle"
    await p.stop()
    assert p._state == "stopped"
    assert p._task is None


async def test_server_start_creates_server_and_task(tmp_path):
    p = ClientServerProvider()
    ctx = make_context(tmp_path, mode="server", port=0)
    # port=0 lets the OS pick a free port
    ctx = SyncContext(
        pair_id="test",
        local=str(tmp_path),
        direction="bidirectional",
        interval=60,
        provider_config={"mode": "server", "host": "127.0.0.1", "port": 0},
    )
    await p.start(ctx)
    assert p._server is not None
    assert p._task is not None
    await p.stop()
    assert p._server is None
    assert p._task is None


async def test_pause_and_resume(tmp_path):
    p = ClientServerProvider()
    await p.start(make_context(tmp_path, mode="client", port=19877))
    await p.pause()
    assert not p._paused.is_set()
    assert p._state == "paused"
    await p.resume()
    assert p._paused.is_set()
    assert p._state == "idle"
    await p.stop()


async def test_status_before_start():
    p = ClientServerProvider()
    s = await p.status()
    assert s.pair_id == ""
    assert s.state == "stopped"
    assert s.last_sync == 0.0


async def test_status_after_start(tmp_path):
    p = ClientServerProvider()
    ctx = make_context(tmp_path, mode="client", port=19878)
    await p.start(ctx)
    s = await p.status()
    assert s.pair_id == "test-pair"
    assert s.state in ("idle", "syncing", "error")
    assert s.extra["mode"] == "client"
    await p.stop()


# --- index initialisation ---

async def test_start_creates_synak_dir(tmp_path):
    p = ClientServerProvider()
    ctx = make_context(tmp_path, mode="client", port=19879)
    await p.start(ctx)
    assert os.path.isdir(os.path.join(str(tmp_path), ".synak"))
    await p.stop()


async def test_index_loaded_on_start(tmp_path):
    # Create a file before starting — it should be indexed
    (tmp_path / "hello.txt").write_bytes(b"hello")
    p = ClientServerProvider()
    ctx = make_context(tmp_path, mode="client", port=19880)
    await p.start(ctx)
    assert p._index is not None
    entry = p._index.get("hello.txt")
    assert entry is not None
    assert not entry.deleted
    await p.stop()


# --- end-to-end: server ↔ client sync ---

async def test_client_pulls_file_from_server(tmp_path):
    server_dir = tmp_path / "server"
    client_dir = tmp_path / "client"
    server_dir.mkdir()
    client_dir.mkdir()

    (server_dir / "shared.txt").write_bytes(b"hello from server")

    server = ClientServerProvider()
    srv_ctx = SyncContext(
        pair_id="srv",
        local=str(server_dir),
        direction="bidirectional",
        interval=9999,
        provider_config={"mode": "server", "host": "127.0.0.1", "port": 0},
    )
    await server.start(srv_ctx)

    # Find the actual bound port
    bound_port = server._server.sockets[0].getsockname()[1]

    client = ClientServerProvider()
    cli_ctx = SyncContext(
        pair_id="cli",
        local=str(client_dir),
        direction="bidirectional",
        interval=9999,
        provider_config={"mode": "client", "host": "127.0.0.1", "port": bound_port},
    )
    await client.start(cli_ctx)

    # Trigger a sync and wait for it to complete
    await client._run_client_session()
    await asyncio.sleep(0.05)

    assert (client_dir / "shared.txt").exists()
    assert (client_dir / "shared.txt").read_bytes() == b"hello from server"

    await client.stop()
    await server.stop()


async def test_client_pushes_file_to_server(tmp_path):
    server_dir = tmp_path / "server"
    client_dir = tmp_path / "client"
    server_dir.mkdir()
    client_dir.mkdir()

    (client_dir / "from_client.txt").write_bytes(b"client data")

    server = ClientServerProvider()
    srv_ctx = SyncContext(
        pair_id="srv",
        local=str(server_dir),
        direction="bidirectional",
        interval=9999,
        provider_config={"mode": "server", "host": "127.0.0.1", "port": 0},
    )
    await server.start(srv_ctx)
    bound_port = server._server.sockets[0].getsockname()[1]

    client = ClientServerProvider()
    cli_ctx = SyncContext(
        pair_id="cli",
        local=str(client_dir),
        direction="bidirectional",
        interval=9999,
        provider_config={"mode": "client", "host": "127.0.0.1", "port": bound_port},
    )
    await client.start(cli_ctx)
    await client._run_client_session()
    await asyncio.sleep(0.1)

    assert (server_dir / "from_client.txt").exists()
    assert (server_dir / "from_client.txt").read_bytes() == b"client data"

    await client.stop()
    await server.stop()


async def test_connection_refused_sets_error_state(tmp_path):
    p = ClientServerProvider()
    ctx = SyncContext(
        pair_id="cli",
        local=str(tmp_path),
        direction="bidirectional",
        interval=9999,
        provider_config={"mode": "client", "host": "127.0.0.1", "port": 19999},
    )
    await p.start(ctx)
    await p._run_client_session()
    assert p._state == "error"
    assert "Cannot connect" in p._error
    await p.stop()
