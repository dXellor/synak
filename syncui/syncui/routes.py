"""Flask route definitions for syncui."""

from __future__ import annotations

import json as _json

from flask import Flask, current_app, jsonify, redirect, render_template, request, session, url_for

from syncui.daemon_client import DaemonClient, DaemonNotRunningError, DaemonError
from syncui.ipc import default_socket_address
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


def _socket_address() -> str:
    return (
        session.get("daemon_socket")
        or current_app.config.get("DAEMON_SOCKET_DEFAULT")
        or default_socket_address()
    )


def _client() -> DaemonClient:
    return DaemonClient(_socket_address())


def register(app: Flask) -> None:
    app.add_url_rule("/", view_func=index)
    app.add_url_rule("/connect", view_func=connect, methods=["POST"])
    app.add_url_rule("/fragment/form", view_func=fragment_form, methods=["POST"])
    app.add_url_rule("/api/config", view_func=api_config_get, methods=["GET"])
    app.add_url_rule("/api/config", view_func=api_config_post, methods=["POST"])
    app.add_url_rule("/api/convert", view_func=api_convert, methods=["POST"])
    app.add_url_rule("/api/status", view_func=api_status, methods=["GET"])
    app.add_url_rule("/api/schemas", view_func=api_schemas, methods=["GET"])


def connect():
    address = request.form.get("address", "").strip()
    if address:
        session["daemon_socket"] = address
    else:
        session.pop("daemon_socket", None)
    return redirect(url_for("index"))


def index():
    config = None
    toml_str = ""
    daemon_down = False
    daemon_error = None

    try:
        config = _client().get("/config")
        toml_str = dict_to_toml(config)
    except DaemonNotRunningError as e:
        daemon_down = True
        daemon_error = str(e)
    except DaemonError as e:
        daemon_error = str(e)

    return render_template(
        "index.html",
        config=config,
        toml=toml_str,
        schemas=_PROVIDER_SCHEMAS,
        schemas_json=_json.dumps(_PROVIDER_SCHEMAS),
        daemon_down=daemon_down,
        daemon_error=daemon_error,
        current_socket=_socket_address(),
    )


def fragment_form():
    """Return just the form pane HTML for a config dict POSTed as JSON."""
    try:
        config = request.get_json(silent=True) or {}
    except Exception:
        return "bad request", 400
    return render_template(
        "_form_fragment.html",
        config=config,
        schemas=_PROVIDER_SCHEMAS,
    )


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


def api_convert():
    """Convert between JSON dict and TOML string without touching the daemon."""
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"error": "request body must be JSON"}), 400
    if "toml" in body:
        try:
            d = toml_to_dict(body["toml"])
        except ValueError as e:
            return jsonify({"error": str(e)}), 422
        return jsonify({"json": d})
    if "json" in body:
        try:
            t = dict_to_toml(body["json"])
        except Exception as e:
            return jsonify({"error": str(e)}), 422
        return jsonify({"toml": t})
    return jsonify({"error": "body must contain 'toml' or 'json' key"}), 400


def api_schemas():
    return jsonify(_PROVIDER_SCHEMAS)
