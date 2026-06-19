"""Flask route definitions for syncui."""

from __future__ import annotations

from flask import Flask, current_app, jsonify, request

from syncui.daemon_client import DaemonClient, DaemonNotRunningError, DaemonError
from syncui.tomlio import dict_to_toml, toml_to_dict

# Provider schemas embedded here so the UI works even when the daemon is down.
_PROVIDER_SCHEMAS: dict[str, dict] = {
    "client-server": {
        "required": ["mode", "port"],
        "properties": {
            "mode": {"type": "string", "enum": ["server", "client"], "description": "'server' listens; 'client' connects"},
            "host": {"type": "string", "description": "Bind address (server) or server hostname (client)"},
            "port": {"type": "integer", "description": "TCP port"},
            "node_id": {"type": "string", "description": "Human-readable node name"},
            "conflict_strategy": {"type": "string", "enum": ["last-write-wins", "keep-both"]},
            "sync_deletes": {"type": "boolean", "description": "Propagate remote deletions. Default true."},
            "verify_interval": {"type": "integer", "description": "Seconds between integrity verify passes. 0 = disabled."},
            "verify_sleep": {"type": "number", "description": "Seconds between per-file hashes during verify. Default 0.1."},
        },
    },
    "p2p": {
        "required": ["peers"],
        "properties": {
            "peers": {"type": "array", "items": {"type": "string"}, "description": "Peer addresses as 'host' or 'host:port'"},
            "port": {"type": "integer", "description": "Explicit listen port"},
            "node_id": {"type": "string", "description": "Human-readable node name"},
            "conflict_strategy": {"type": "string", "enum": ["last-write-wins", "keep-both"]},
            "sync_deletes": {"type": "boolean", "description": "Propagate remote deletions. Default true."},
            "verify_interval": {"type": "integer", "description": "Seconds between integrity verify passes. 0 = disabled."},
            "verify_sleep": {"type": "number", "description": "Seconds between per-file hashes during verify. Default 0.1."},
        },
    },
}


def _client() -> DaemonClient:
    return DaemonClient(current_app.config.get("DAEMON_SOCKET"))


def register(app: Flask) -> None:
    app.add_url_rule("/", view_func=index)
    app.add_url_rule("/api/config", view_func=api_config_get, methods=["GET"])
    app.add_url_rule("/api/config", view_func=api_config_post, methods=["POST"])
    app.add_url_rule("/api/status", view_func=api_status, methods=["GET"])
    app.add_url_rule("/api/schemas", view_func=api_schemas, methods=["GET"])


def index():
    from flask import render_template
    return render_template("index.html")


def api_config_get():
    try:
        config_dict = _client().get("/config")
    except DaemonNotRunningError as e:
        return jsonify({"error": str(e), "daemon_down": True}), 503
    except DaemonError as e:
        return jsonify({"error": str(e)}), e.status

    try:
        toml_str = dict_to_toml(config_dict)
    except Exception as e:
        toml_str = f"# Failed to render TOML: {e}"

    return jsonify({"json": config_dict, "toml": toml_str})


def api_config_post():
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"error": "request body must be JSON"}), 400

    config_dict = None

    if "toml" in body:
        try:
            config_dict = toml_to_dict(body["toml"])
        except ValueError as e:
            return jsonify({"error": str(e)}), 422
    elif "json" in body:
        config_dict = body["json"]
    else:
        return jsonify({"error": "body must contain 'toml' or 'json' key"}), 400

    try:
        result = _client().post("/config/apply", body=config_dict)
    except DaemonNotRunningError as e:
        return jsonify({"error": str(e), "daemon_down": True}), 503
    except DaemonError as e:
        return jsonify({"error": str(e)}), e.status

    try:
        toml_str = dict_to_toml(result)
    except Exception:
        toml_str = ""

    return jsonify({"json": result, "toml": toml_str})


def api_status():
    try:
        return jsonify(_client().get("/status"))
    except DaemonNotRunningError as e:
        return jsonify({"error": str(e), "daemon_down": True}), 503
    except DaemonError as e:
        return jsonify({"error": str(e)}), e.status


def api_schemas():
    return jsonify(_PROVIDER_SCHEMAS)
