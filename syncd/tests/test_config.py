import os
import pytest
import tomllib

from syncd.config import (
    AppConfig,
    ConfigError,
    DaemonConfig,
    PairConfig,
    load_config,
)


def write_toml(tmp_path, content: str) -> str:
    p = tmp_path / "config.toml"
    p.write_text(content)
    return str(p)


def test_minimal_config(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
id = "photos"
mode = "p2p"
local = "/tmp/photos"
direction = "push"
""")
    cfg = load_config(path)
    assert isinstance(cfg, AppConfig)
    assert len(cfg.pairs) == 1
    assert cfg.pairs[0].id == "photos"
    assert cfg.pairs[0].mode == "p2p"
    assert cfg.pairs[0].local == "/tmp/photos"
    assert cfg.pairs[0].direction == "push"
    assert cfg.pairs[0].interval == 0


def test_daemon_defaults(tmp_path):
    path = write_toml(tmp_path, "")
    cfg = load_config(path)
    assert cfg.daemon.log_level == "info"
    assert str(os.getuid()) in cfg.daemon.api_socket


def test_daemon_explicit(tmp_path):
    path = write_toml(tmp_path, """
[daemon]
api_socket = "/tmp/test.sock"
log_level = "debug"
""")
    cfg = load_config(path)
    assert cfg.daemon.api_socket == "/tmp/test.sock"
    assert cfg.daemon.log_level == "debug"


def test_local_path_expanded(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
id = "docs"
mode = "client-server"
local = "~/Documents"
direction = "pull"
""")
    cfg = load_config(path)
    assert not cfg.pairs[0].local.startswith("~")
    assert cfg.pairs[0].local == os.path.expanduser("~/Documents")


def test_provider_config_passed_through(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
id = "work"
mode = "client-server"
local = "/tmp/work"
direction = "bidirectional"

[pairs.provider]
remote = "user@host:/work"
""")
    cfg = load_config(path)
    assert cfg.pairs[0].provider == {"remote": "user@host:/work"}


def test_multiple_pairs(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
id = "a"
mode = "p2p"
local = "/tmp/a"
direction = "push"

[[pairs]]
id = "b"
mode = "client-server"
local = "/tmp/b"
direction = "pull"
""")
    cfg = load_config(path)
    assert len(cfg.pairs) == 2
    assert cfg.pairs[0].id == "a"
    assert cfg.pairs[1].id == "b"


def test_missing_id_raises(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
mode = "p2p"
local = "/tmp/x"
direction = "push"
""")
    with pytest.raises(ConfigError, match="id"):
        load_config(path)


def test_missing_mode_raises(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
id = "x"
local = "/tmp/x"
direction = "push"
""")
    with pytest.raises(ConfigError, match="mode"):
        load_config(path)


def test_missing_local_raises(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
id = "x"
mode = "p2p"
direction = "push"
""")
    with pytest.raises(ConfigError, match="local"):
        load_config(path)


def test_missing_direction_raises(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
id = "x"
mode = "p2p"
local = "/tmp/x"
""")
    with pytest.raises(ConfigError, match="direction"):
        load_config(path)


def test_invalid_direction_raises(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
id = "x"
mode = "p2p"
local = "/tmp/x"
direction = "sideways"
""")
    with pytest.raises(ConfigError, match="direction"):
        load_config(path)


def test_invalid_log_level_raises(tmp_path):
    path = write_toml(tmp_path, """
[daemon]
log_level = "verbose"
""")
    with pytest.raises(ConfigError, match="log_level"):
        load_config(path)


def test_file_not_found_raises():
    with pytest.raises(ConfigError, match="not found"):
        load_config("/nonexistent/path/config.toml")


def test_invalid_toml_raises(tmp_path):
    path = str(tmp_path / "config.toml")
    with open(path, "w") as f:
        f.write("this is not [ valid toml !!!]")
    with pytest.raises(ConfigError, match="TOML"):
        load_config(path)


def test_interval_default_zero(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
id = "x"
mode = "p2p"
local = "/tmp/x"
direction = "push"
""")
    cfg = load_config(path)
    assert cfg.pairs[0].interval == 0


def test_interval_explicit(tmp_path):
    path = write_toml(tmp_path, """
[[pairs]]
id = "x"
mode = "p2p"
local = "/tmp/x"
direction = "push"
interval = 300
""")
    cfg = load_config(path)
    assert cfg.pairs[0].interval == 300


def test_peers_defaults(tmp_path):
    path = write_toml(tmp_path, "")
    cfg = load_config(path)
    assert cfg.peers.discovery == "static"
    assert cfg.peers.static == []


def test_peers_explicit(tmp_path):
    path = write_toml(tmp_path, """
[peers]
discovery = "mdns"

[[peers.static]]
id = "peer-abc"
address = "192.168.1.10:51820"
""")
    cfg = load_config(path)
    assert cfg.peers.discovery == "mdns"
    assert cfg.peers.static == [{"id": "peer-abc", "address": "192.168.1.10:51820"}]
