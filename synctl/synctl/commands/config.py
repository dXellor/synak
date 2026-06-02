import asyncio
import json as json_mod
import sys

import click

from synctl.client import DaemonClient, DaemonError, DaemonNotRunningError


@click.group("config")
def config_cmd() -> None:
    """Manage daemon configuration."""


def _client(ctx: click.Context) -> DaemonClient:
    return DaemonClient(ctx.obj["socket"])


def _run(coro):
    try:
        return asyncio.run(coro)
    except DaemonNotRunningError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    except DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)


@config_cmd.command("show")
@click.pass_context
def show(ctx: click.Context) -> None:
    """Show current loaded daemon config."""
    data = _run(_client(ctx).get("/config"))
    click.echo(json_mod.dumps(data, indent=2))


@config_cmd.command("reload")
@click.pass_context
def reload_config(ctx: click.Context) -> None:
    """Tell daemon to reload config from disk."""
    _run(_client(ctx).post("/config/reload"))
    click.echo("config reload requested")
