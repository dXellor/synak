import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from syncd.sync.base import SyncContext
from syncd.sync.providers.client_server import ClientServerProvider


def make_context(interval=60, direction="push") -> SyncContext:
    return SyncContext(
        pair_id="test-pair",
        local="/tmp/local",
        direction=direction,
        interval=interval,
        provider_config={"remote": "user@host:/remote"},
    )


def make_proc(returncode=0, stderr=b""):
    proc = MagicMock()
    proc.returncode = returncode
    proc.communicate = AsyncMock(return_value=(b"", stderr))
    return proc


# --- lifecycle ---

async def test_start_creates_task():
    p = ClientServerProvider()
    ctx = make_context()
    with patch("asyncio.create_subprocess_exec", return_value=make_proc()):
        await p.start(ctx)
        assert p._task is not None
        assert p._state == "idle"
        await p.stop()


async def test_stop_cancels_task():
    p = ClientServerProvider()
    await p.start(make_context())
    await p.stop()
    assert p._state == "stopped"
    assert p._task is None


async def test_pause_clears_event():
    p = ClientServerProvider()
    await p.start(make_context())
    await p.pause()
    assert not p._paused.is_set()
    assert p._state == "paused"
    await p.stop()


async def test_resume_sets_event():
    p = ClientServerProvider()
    await p.start(make_context())
    await p.pause()
    await p.resume()
    assert p._paused.is_set()
    assert p._state == "idle"
    await p.stop()


# --- rsync success ---

async def test_run_rsync_success_sets_idle():
    p = ClientServerProvider()
    p._context = make_context()
    p._state = "idle"

    with patch("asyncio.create_subprocess_exec", return_value=make_proc(returncode=0)):
        await p._run_rsync()

    assert p._state == "idle"
    assert p._last_sync > 0
    assert p._error == ""


async def test_run_rsync_failure_sets_error():
    p = ClientServerProvider()
    p._context = make_context()
    p._state = "idle"

    with patch(
        "asyncio.create_subprocess_exec",
        return_value=make_proc(returncode=1, stderr=b"rsync: connection failed"),
    ):
        await p._run_rsync()

    assert p._state == "error"
    assert "connection failed" in p._error
    assert p._last_sync == 0.0


# --- status ---

async def test_status_reflects_state():
    p = ClientServerProvider()
    p._context = make_context()
    p._state = "idle"
    p._last_sync = 1234.5
    p._error = ""

    s = await p.status()
    assert s.pair_id == "test-pair"
    assert s.state == "idle"
    assert s.last_sync == 1234.5
    assert s.error == ""


async def test_status_before_start():
    p = ClientServerProvider()
    s = await p.status()
    assert s.pair_id == ""
    assert s.state == "stopped"


# --- rsync args ---

def test_build_rsync_args_push():
    p = ClientServerProvider()
    ctx = make_context(direction="push")
    args = p._build_rsync_args(ctx)
    assert args[0] == "rsync"
    assert "/tmp/local/" in args
    assert "user@host:/remote" in args
    assert args.index("/tmp/local/") < args.index("user@host:/remote")


def test_build_rsync_args_pull():
    p = ClientServerProvider()
    ctx = make_context(direction="pull")
    args = p._build_rsync_args(ctx)
    assert "user@host:/remote/" in args
    assert "/tmp/local" in args
    assert args.index("user@host:/remote/") < args.index("/tmp/local")


def test_build_rsync_args_bidirectional_is_push():
    p = ClientServerProvider()
    ctx = make_context(direction="bidirectional")
    args = p._build_rsync_args(ctx)
    # bidirectional falls back to push for now
    assert "/tmp/local/" in args
    assert "user@host:/remote" in args


def test_build_rsync_args_includes_base_flags():
    p = ClientServerProvider()
    args = p._build_rsync_args(make_context())
    assert "-az" in args
    assert "--delete" in args


# --- interval loop ---

async def test_interval_loop_calls_rsync_then_sleeps():
    p = ClientServerProvider()
    p._context = make_context(interval=100)
    p._state = "idle"

    call_count = 0

    async def fake_rsync():
        nonlocal call_count
        call_count += 1
        p._state = "idle"

    async def fake_sleep(n):
        raise asyncio.CancelledError  # stop after first iteration

    with patch.object(p, "_run_rsync", fake_rsync), \
         patch("asyncio.sleep", fake_sleep):
        with pytest.raises(asyncio.CancelledError):
            await p._interval_loop()

    assert call_count == 1


# --- trigger ---

async def test_trigger_creates_task():
    p = ClientServerProvider()
    p._context = make_context()
    p._state = "idle"

    with patch("asyncio.create_subprocess_exec", return_value=make_proc(returncode=0)):
        await p.start(p._context)
        await p.trigger()
        await asyncio.sleep(0)  # let trigger task run
        await p.stop()
