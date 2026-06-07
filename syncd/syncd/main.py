import argparse
import asyncio
import logging
import os
import sys

from syncd.config import ConfigError, DEFAULT_CONFIG_PATH, load_config
from syncd.daemon import Daemon


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="syncd", description="File sync daemon")
    p.add_argument(
        "-c", "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Path to config file (default: %(default)s)",
    )
    p.add_argument(
        "--log-level",
        default=None,
        metavar="LEVEL",
        help="Override log level from config (debug/info/warning/error)",
    )
    p.add_argument(
        "-d", "--detach",
        action="store_true",
        help="Detach from terminal and run in the background",
    )
    return p


def _default_log_path(socket_path: str) -> str:
    return socket_path.removesuffix(".sock") + ".log"


def _daemonize(log_path: str) -> None:
    """Double-fork to detach from the controlling terminal.

    Returns only in the grandchild process with stdin → /dev/null and
    stdout/stderr → log_path.
    """
    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    os.setsid()

    pid = os.fork()
    if pid > 0:
        sys.exit(0)

    # Redirect stdin to /dev/null, stdout/stderr to the log file
    sys.stdout.flush()
    sys.stderr.flush()
    devnull = os.open(os.devnull, os.O_RDONLY)
    log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.dup2(devnull, sys.stdin.fileno())
    os.dup2(log_fd, sys.stdout.fileno())
    os.dup2(log_fd, sys.stderr.fileno())
    os.close(devnull)
    os.close(log_fd)


def main() -> None:
    args = build_parser().parse_args()

    try:
        config = load_config(args.config)
    except ConfigError as e:
        print(f"syncd: config error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.detach:
        log_path = _default_log_path(config.daemon.api_socket)
        print(f"syncd: starting in background — logs: {log_path}")
        sys.stdout.flush()
        _daemonize(log_path)

    log_level = (args.log_level or config.daemon.log_level).upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    daemon = Daemon(config, config_path=args.config)
    asyncio.run(daemon.run())


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
