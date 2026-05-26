import asyncio
import signal
import pytest

from syncd.config import AppConfig, DaemonConfig, PeersConfig
from syncd.daemon import Daemon


def make_config() -> AppConfig:
    return AppConfig(
        daemon=DaemonConfig(api_socket="/tmp/test.sock", log_level="info"),
        pairs=[],
        peers=PeersConfig(discovery="static", static=[]),
    )


async def test_run_starts_and_stops():
    daemon = Daemon(make_config())

    async def trigger_shutdown():
        await asyncio.sleep(0)
        daemon._handle_sigterm()

    await asyncio.gather(daemon.run(), trigger_shutdown())


async def test_sigterm_sets_shutdown_event():
    daemon = Daemon(make_config())
    daemon._shutdown_event = asyncio.Event()
    daemon._handle_sigterm()
    assert daemon._shutdown_event.is_set()


async def test_reload_config_no_path_logs_warning(caplog):
    import logging
    daemon = Daemon(make_config(), config_path="")
    with caplog.at_level(logging.WARNING, logger="syncd.daemon"):
        await daemon._reload_config()
    assert "No config path" in caplog.text


async def test_reload_config_bad_path_logs_error(caplog, tmp_path):
    import logging
    daemon = Daemon(make_config(), config_path=str(tmp_path / "missing.toml"))
    with caplog.at_level(logging.ERROR, logger="syncd.daemon"):
        await daemon._reload_config()
    assert "reload failed" in caplog.text


async def test_reload_config_success(tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text('[daemon]\nlog_level = "debug"\n')
    daemon = Daemon(make_config(), config_path=str(config_file))
    await daemon._reload_config()
    assert daemon._config.daemon.log_level == "debug"


async def test_startup_and_shutdown_stubs_do_not_raise():
    daemon = Daemon(make_config())
    await daemon._startup()
    await daemon._shutdown()
