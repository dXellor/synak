from flask import Flask


def create_app(socket_path: str | None = None) -> Flask:
    app = Flask(__name__)
    app.config["DAEMON_SOCKET"] = socket_path

    from sync_web import routes
    routes.register(app)

    return app


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="sync-web: web UI for syncd")
    parser.add_argument("--socket", help="Path to syncd Unix socket")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    app = create_app(socket_path=args.socket)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
