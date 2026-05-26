import asyncio
import json as json_mod
import sys

import click

from synctl.client import DaemonClient, DaemonNotRunningError


@click.command("status")
@click.pass_context
def status_cmd(ctx: click.Context) -> None:
    """Show daemon status and active sync pairs."""
    try:
        data = asyncio.run(_get_status(ctx.obj["socket"]))
    except DaemonNotRunningError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)

    if ctx.obj["json"]:
        click.echo(json_mod.dumps(data, indent=2))
    else:
        _pretty(data)


async def _get_status(socket: str | None) -> dict:
    return await DaemonClient(socket).get("/status")


def _pretty(data: dict) -> None:
    click.echo(f"syncd v{data.get('version', '?')}  uptime {data.get('uptime', '?')}s")
    pairs = data.get("pairs", [])
    if not pairs:
        click.echo("  no active pairs")
        return
    for p in pairs:
        err = f"  [{p['error']}]" if p.get("error") else ""
        click.echo(f"  {p['pair_id']:<24} {p['state']}{err}")
