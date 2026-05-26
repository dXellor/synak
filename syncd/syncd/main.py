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
    return p


def main() -> None:
    args = build_parser().parse_args()

    try:
        config = load_config(args.config)
    except ConfigError as e:
        print(f"syncd: config error: {e}", file=sys.stderr)
        sys.exit(1)

    log_level = (args.log_level or config.daemon.log_level).upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    daemon = Daemon(config, config_path=args.config)
    asyncio.run(daemon.run())


if __name__ == "__main__":
    main()
