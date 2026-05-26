import logging

import aiohttp.web as web

from syncd.api.routes import build_routes
from syncd.utils.fs import remove_socket

logger = logging.getLogger(__name__)


class ApiServer:
    def __init__(self, socket_path: str) -> None:
        self._socket_path = socket_path
        self._runner: web.AppRunner | None = None

    async def start(self, app_state: dict) -> None:
        app = web.Application()
        app["state"] = app_state
        app.add_routes(build_routes())

        await remove_socket(self._socket_path)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.UnixSite(self._runner, self._socket_path)
        await site.start()
        logger.info("API listening on %s", self._socket_path)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
            logger.info("API server stopped")
