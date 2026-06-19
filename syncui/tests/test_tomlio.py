"""Round-trip tests for JSON<->TOML conversion."""

import pytest
from syncui.tomlio import dict_to_toml, toml_to_dict

FULL_CONFIG = {
    "daemon": {
        "api_socket": "/run/user/1000/syncd.sock",
        "log_level": "info",
    },
    "pairs": [
        {
            "id": "work-docs",
            "mode": "client-server",
            "local": "/home/user/Documents/Work",
            "direction": "bidirectional",
            "interval": 60,
            "exclude": ["*.log", "*.pdf"],
            "provider": {
                "mode": "client",
                "host": "myserver.example.com",
                "port": 5000,
                "conflict_strategy": "keep-both",
                "sync_deletes": False,
            },
        },
        {
            "id": "photos",
            "mode": "p2p",
            "local": "/home/user/Pictures",
            "direction": "bidirectional",
            "interval": 30,
            "exclude": [],
            "provider": {
                "peers": ["192.168.1.10:5001", "192.168.1.11:5001"],
                "port": 5001,
                "node_id": "laptop",
            },
        },
    ],
    "peers": {
        "discovery": "static",
        "static": [],
    },
}


def test_dict_to_toml_roundtrip():
    toml_str = dict_to_toml(FULL_CONFIG)
    assert isinstance(toml_str, str)
    assert len(toml_str) > 0
    recovered = toml_to_dict(toml_str)
    assert recovered["daemon"]["log_level"] == "info"
    assert recovered["pairs"][0]["id"] == "work-docs"
    assert recovered["pairs"][1]["provider"]["peers"] == ["192.168.1.10:5001", "192.168.1.11:5001"]


def test_toml_to_dict_invalid():
    with pytest.raises(ValueError, match="Invalid TOML"):
        toml_to_dict("this is not valid toml ===")


def test_sanitize_removes_none():
    data = {"key": None, "other": "value"}
    toml_str = dict_to_toml(data)
    recovered = toml_to_dict(toml_str)
    assert "key" not in recovered
    assert recovered["other"] == "value"


def test_sanitize_converts_tuples():
    data = {"items": ("a", "b", "c")}
    toml_str = dict_to_toml(data)
    recovered = toml_to_dict(toml_str)
    assert recovered["items"] == ["a", "b", "c"]
