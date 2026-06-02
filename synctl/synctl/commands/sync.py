import asyncio
import json as json_mod
import sys

import click

from synctl.client import DaemonClient, DaemonError, DaemonNotRunningError


@click.group("sync")
def sync_cmd() -> None:
    """Manage sync pairs."""


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


@sync_cmd.command("list")
@click.pass_context
def list_pairs(ctx: click.Context) -> None:
    """List all configured sync pairs."""
    data = _run(_client(ctx).get("/pairs"))
    if ctx.obj["json"]:
        click.echo(json_mod.dumps(data, indent=2))
    else:
        if not data:
            click.echo("no pairs configured")
            return
        for p in data:
            status = p.get("status") or {}
            state = status.get("state", "unknown")
            click.echo(f"  {p['id']:<24} {p['mode']:<16} {state}")


@sync_cmd.command("trigger")
@click.argument("pair_id")
@click.pass_context
def trigger(ctx: click.Context, pair_id: str) -> None:
    """Trigger a manual sync for PAIR_ID."""
    _run(_client(ctx).post(f"/pairs/{pair_id}/sync"))
    click.echo(f"triggered sync for {pair_id!r}")


@sync_cmd.command("pause")
@click.argument("pair_id")
@click.pass_context
def pause(ctx: click.Context, pair_id: str) -> None:
    """Pause syncing for PAIR_ID."""
    _run(_client(ctx).post(f"/pairs/{pair_id}/pause"))
    click.echo(f"paused {pair_id!r}")


@sync_cmd.command("resume")
@click.argument("pair_id")
@click.pass_context
def resume(ctx: click.Context, pair_id: str) -> None:
    """Resume syncing for PAIR_ID."""
    _run(_client(ctx).post(f"/pairs/{pair_id}/resume"))
    click.echo(f"resumed {pair_id!r}")
