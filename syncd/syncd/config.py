import os
import tomllib
from dataclasses import dataclass, field
from typing import Any

DEFAULT_CONFIG_PATH = os.path.expanduser("~/.config/syncd/config.toml")

_VALID_DIRECTIONS = {"push", "pull", "bidirectional"}
_VALID_LOG_LEVELS = {"debug", "info", "warning", "error", "critical"}


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class DaemonConfig:
    api_socket: str
    log_level: str


@dataclass(frozen=True)
class PairConfig:
    id: str
    mode: str
    local: str
    direction: str
    interval: int
    provider: dict[str, Any]


@dataclass(frozen=True)
class PeersConfig:
    discovery: str
    static: list[dict[str, str]]


@dataclass(frozen=True)
class AppConfig:
    daemon: DaemonConfig
    pairs: list[PairConfig]
    peers: PeersConfig


def _default_socket() -> str:
    return f"/run/user/{os.getuid()}/syncd.sock"


def _parse_daemon(raw: dict[str, Any]) -> DaemonConfig:
    socket = raw.get("api_socket", _default_socket())
    log_level = raw.get("log_level", "info")
    if log_level not in _VALID_LOG_LEVELS:
        raise ConfigError(
            f"[daemon] log_level must be one of {sorted(_VALID_LOG_LEVELS)}, got {log_level!r}"
        )
    return DaemonConfig(api_socket=socket, log_level=log_level)


def _parse_pair(raw: dict[str, Any], index: int) -> PairConfig:
    ctx = f"[[pairs]] entry {index}"
    for required in ("id", "mode", "local", "direction"):
        if required not in raw:
            raise ConfigError(f"{ctx}: missing required field {required!r}")

    direction = raw["direction"]
    if direction not in _VALID_DIRECTIONS:
        raise ConfigError(
            f"{ctx}: direction must be one of {sorted(_VALID_DIRECTIONS)}, got {direction!r}"
        )

    local = os.path.expanduser(raw["local"])
    interval = raw.get("interval", 0)
    if not isinstance(interval, int) or interval < 0:
        raise ConfigError(f"{ctx}: interval must be a non-negative integer")

    provider = dict(raw.get("provider", {}))
    return PairConfig(
        id=raw["id"],
        mode=raw["mode"],
        local=local,
        direction=direction,
        interval=interval,
        provider=provider,
    )


def _parse_peers(raw: dict[str, Any]) -> PeersConfig:
    discovery = raw.get("discovery", "static")
    static_list = raw.get("static", [])
    if not isinstance(static_list, list):
        raise ConfigError("[peers] static must be an array")
    return PeersConfig(discovery=discovery, static=list(static_list))


def load_config(path: str = DEFAULT_CONFIG_PATH) -> AppConfig:
    """Load and validate config from a TOML file. Raises ConfigError on failure."""
    try:
        with open(path, "rb") as f:
            raw = tomllib.load(f)
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {path}")
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"Invalid TOML in {path}: {e}")

    daemon = _parse_daemon(raw.get("daemon", {}))

    pairs_raw = raw.get("pairs", [])
    if not isinstance(pairs_raw, list):
        raise ConfigError("'pairs' must be an array of tables")
    pairs = [_parse_pair(p, i) for i, p in enumerate(pairs_raw)]

    peers = _parse_peers(raw.get("peers", {}))

    return AppConfig(daemon=daemon, pairs=pairs, peers=peers)
