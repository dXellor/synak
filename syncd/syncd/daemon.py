import asyncio
import logging
import signal

from syncd.config import AppConfig, ConfigError, load_config

logger = logging.getLogger(__name__)


class Daemon:
    def __init__(self, config: AppConfig, config_path: str = "") -> None:
        self._config = config
        self._config_path = config_path
        self._shutdown_event: asyncio.Event | None = None

    async def run(self) -> None:
        self._shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, self._handle_sigterm)
        loop.add_signal_handler(signal.SIGHUP, self._handle_sighup)

        logger.info("syncd starting")
        try:
            await self._startup()
            await self._shutdown_event.wait()
        finally:
            await self._shutdown()
            logger.info("syncd stopped")

    async def _startup(self) -> None:
        pass

    async def _shutdown(self) -> None:
        pass

    def _handle_sigterm(self) -> None:
        logger.info("SIGTERM received, shutting down")
        if self._shutdown_event:
            self._shutdown_event.set()

    def _handle_sighup(self) -> None:
        logger.info("SIGHUP received, reloading config")
        loop = asyncio.get_running_loop()
        loop.create_task(self._reload_config())

    async def _reload_config(self) -> None:
        if not self._config_path:
            logger.warning("No config path set, cannot reload")
            return
        try:
            self._config = load_config(self._config_path)
            logger.info("Config reloaded from %s", self._config_path)
        except ConfigError as e:
            logger.error("Config reload failed: %s", e)
