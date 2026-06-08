"""Platform-specific signal handler registration."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import Callable, Coroutine, Any

logger = logging.getLogger(__name__)

_IS_WINDOWS = sys.platform == "win32"


def register_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    on_terminate: Callable[[], None],
    on_reload: Callable[[], Coroutine[Any, Any, None]] | None = None,
) -> None:
    """Register OS signal handlers.

    on_terminate — called when the process should shut down (SIGTERM).
    on_reload    — async method to call on SIGHUP (Unix only).

    On Windows, only SIGTERM is handled. SIGHUP does not exist; use
    `synctl config reload` (POST /config/reload) instead.
    """
    if _IS_WINDOWS:
        signal.signal(signal.SIGTERM, lambda *_: on_terminate())
        if on_reload is not None:
            logger.warning(
                "SIGHUP is not available on Windows — "
                "use 'synctl config reload' to reload config without restarting"
            )
        return

    loop.add_signal_handler(signal.SIGTERM, on_terminate)
    if on_reload is not None:
        loop.add_signal_handler(
            signal.SIGHUP,
            lambda: loop.create_task(on_reload()),
        )
