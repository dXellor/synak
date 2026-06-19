"""JSON dict <-> TOML string conversion for syncd config."""

from __future__ import annotations

import tomllib
import tomli_w
from typing import Any


def dict_to_toml(data: dict[str, Any]) -> str:
    """Convert a syncd config dict (from GET /config) to a TOML string."""
    # The daemon emits lists for exclude/peers static; tomli_w handles those fine.
    # PairConfig.exclude comes as a list from dataclasses.asdict.
    # We need to convert any tuple values (shouldn't happen via JSON but guard anyway).
    data = _sanitize(data)
    return tomli_w.dumps(data)


def toml_to_dict(toml_str: str) -> dict[str, Any]:
    """Parse a TOML string back into a config dict. Raises ValueError on parse error."""
    try:
        return tomllib.loads(toml_str)
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"Invalid TOML: {e}") from e


def _sanitize(obj: Any) -> Any:
    """Recursively convert tuples to lists and drop None values for tomli_w."""
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(item) for item in obj]
    return obj
