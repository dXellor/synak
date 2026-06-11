import asyncio
import logging

from syncd.config import AppConfig, ConfigError, load_config
from syncd.platform.signals import register_signal_handlers
from syncd.sync.manager import SyncManager
from syncd.api.server import ApiServer

logger = logging.getLogger(__name__)


class Daemon:
    def __init__(self, config: AppConfig, config_path: str = "") -> None:
        self._config = config
        self._config_path = config_path
        self._shutdown_event: asyncio.Event | None = None
        self._api: ApiServer | None = None
        self._manager: SyncManager | None = None
        self._app_state: dict = {}

    async def run(self) -> None:
        self._shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        register_signal_handlers(loop, self._handle_sigterm, self._reload_config)

        logger.info("syncd starting")
        try:
            await self._startup()
            await self._shutdown_event.wait()
        finally:
            await self._shutdown()
            logger.info("syncd stopped")

    async def _startup(self) -> None:
        import syncd.sync.providers  # noqa: F401 — triggers provider auto-registration

        self._manager = SyncManager()
        await self._manager.start_all(self._config.pairs)

        self._app_state = {
            "daemon": self,
            "config": self._config,
            "manager": self._manager,
        }
        self._api = ApiServer(self._config.daemon.api_socket)
        await self._api.start(self._app_state)

    async def _shutdown(self) -> None:
        if self._api is not None:
            await self._api.stop()
        if self._manager is not None:
            await self._manager.stop_all()

    def _handle_sigterm(self) -> None:
        logger.info("SIGTERM received, shutting down")
        if self._shutdown_event:
            self._shutdown_event.set()

    async def _reload_config(self) -> None:
        if not self._config_path:
            logger.warning("No config path set, cannot reload")
            return
        try:
            new_config = load_config(self._config_path)
        except ConfigError as e:
            logger.error("Config reload failed: %s", e)
            return

        if self._manager is not None:
            await self._manager.stop_all()
        self._manager = SyncManager()
        await self._manager.start_all(new_config.pairs)

        self._config = new_config
        self._app_state["config"] = self._config
        self._app_state["manager"] = self._manager
        logger.info("Config reloaded from %s", self._config_path)
