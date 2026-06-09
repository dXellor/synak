import argparse
import asyncio
import logging
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


def main() -> None:
    args = build_parser().parse_args()

    try:
        config = load_config(args.config)
    except ConfigError as e:
        print(f"syncd: config error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.detach:
        from syncd.platform.process import daemonize, already_detached
        if not already_detached():
            from syncd.platform.ipc import default_log_path
            log_path = default_log_path(config.daemon.api_socket)
            print(f"syncd: starting in background — logs: {log_path}")
            sys.stdout.flush()
            daemonize(log_path)
            # Unix: daemonize() returns here in the grandchild, continue normally
            # Windows: daemonize() exits the parent; the child re-enters main() above

    log_level = (args.log_level or config.daemon.log_level).upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    logging.getLogger("watchfiles").setLevel(logging.WARNING)

    daemon = Daemon(config, config_path=args.config)
    asyncio.run(daemon.run())


if __name__ == "__main__":
    main()
