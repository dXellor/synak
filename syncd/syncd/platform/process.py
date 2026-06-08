"""Platform-specific process daemonization."""

from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)

_IS_WINDOWS = sys.platform == "win32"

# Env var set on the detached child so it doesn't try to detach again
_DETACHED_ENV = "_SYNCD_DETACHED"


def daemonize(log_path: str) -> None:
    """Detach from the controlling terminal and redirect stdio to log_path.

    Unix: double-fork. Returns only in the grandchild process.
    Windows: re-launches the same command as a detached subprocess, then exits.
             The child sees _SYNCD_DETACHED and skips this function entirely.
    """
    if _IS_WINDOWS:
        _daemonize_windows(log_path)
    else:
        _daemonize_unix(log_path)


def already_detached() -> bool:
    """True when running as the detached background child on Windows."""
    return bool(os.environ.get(_DETACHED_ENV))


def _daemonize_windows(log_path: str) -> None:
    import subprocess

    # These flags detach the child from the current console entirely.
    DETACHED_PROCESS = 0x00000008
    CREATE_NEW_PROCESS_GROUP = 0x00000200
    CREATE_NO_WINDOW = 0x08000000

    env = os.environ.copy()
    env[_DETACHED_ENV] = "1"

    with open(log_path, "a") as log_file:
        proc = subprocess.Popen(
            sys.argv,
            env=env,
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.DEVNULL,
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
        )

    print(f"syncd: started in background (pid {proc.pid}) — logs: {log_path}")
    sys.exit(0)


def _daemonize_unix(log_path: str) -> None:
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    os.setsid()

    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    sys.stdout.flush()
    sys.stderr.flush()
    devnull = os.open(os.devnull, os.O_RDONLY)
    log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.dup2(devnull, sys.stdin.fileno())
    os.dup2(log_fd, sys.stdout.fileno())
    os.dup2(log_fd, sys.stderr.fileno())
    os.close(devnull)
    os.close(log_fd)
