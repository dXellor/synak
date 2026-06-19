"""JSON dict <-> TOML string conversion for syncd config."""

from __future__ import annotations

import tomllib
import tomli_w
from typing import Any


def dict_to_toml(data: dict[str, Any]) -> str:
    """Convert a syncd config dict (from GET /config) to a TOML string."""
    data = _sanitize(data)
    return tomli_w.dumps(data)


def toml_to_dict(toml_str: str) -> dict[str, Any]:
    """Parse a TOML string back into a config dict. Raises ValueError on parse error."""
    try:
        return tomllib.loads(toml_str)
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"Invalid TOML: {e}") from e


def _sanitize(obj: Any) -> Any:
    """Recursively convert tuples to lists; drop None values and empty lists/dicts."""
    if isinstance(obj, dict):
        result = {}
        for k, v in obj.items():
            cleaned = _sanitize(v)
            # Drop None, empty lists, and empty dicts — they're just parsed defaults
            if cleaned is None:
                continue
            if isinstance(cleaned, (list, dict)) and len(cleaned) == 0:
                continue
            result[k] = cleaned
        return result
    if isinstance(obj, (list, tuple)):
        return [_sanitize(item) for item in obj]
    return obj
