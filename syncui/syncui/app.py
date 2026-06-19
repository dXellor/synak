from flask import Flask


def create_app(socket_path: str | None = None) -> Flask:
    import os
    app = Flask(__name__)
    app.secret_key = os.environ.get("SYNCUI_SECRET", os.urandom(24))
    app.config["DAEMON_SOCKET_DEFAULT"] = socket_path  # CLI-provided default

    from syncui import routes
    routes.register(app)

    return app


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="syncui: web UI for syncd")
    parser.add_argument("--socket", help="Path to syncd Unix socket")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    app = create_app(socket_path=args.socket)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
