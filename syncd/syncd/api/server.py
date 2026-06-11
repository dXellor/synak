import logging

import aiohttp.web as web

from syncd.api.routes import build_routes
from syncd.platform.ipc import is_unix_socket_address, make_site
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

        if is_unix_socket_address(self._socket_path):
            await remove_socket(self._socket_path)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = await make_site(self._runner, self._socket_path)
        await site.start()
        logger.info("API listening on %s", self._socket_path)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
            logger.info("API server stopped")
