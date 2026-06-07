import asyncio
import sys

import click

from synctl.client import DaemonClient, DaemonError, DaemonNotRunningError


@click.command("shutdown")
@click.pass_context
def shutdown_cmd(ctx: click.Context) -> None:
    """Gracefully shut down the daemon."""
    try:
        asyncio.run(_shutdown(ctx.obj["socket"]))
    except DaemonNotRunningError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    except DaemonError as e:
        click.echo(f"error: {e}", err=True)
        sys.exit(1)
    click.echo("syncd: shutdown requested")


async def _shutdown(socket: str | None) -> None:
    await DaemonClient(socket).post("/shutdown")
