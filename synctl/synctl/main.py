import click

from synctl.commands.status import status_cmd
from synctl.commands.sync import sync_cmd
from synctl.commands.config import config_cmd


@click.group()
@click.option(
    "--socket",
    envvar="SYNCD_SOCKET",
    default=None,
    help="Path to syncd Unix socket (overrides default)",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    default=False,
    help="Output raw JSON",
)
@click.pass_context
def cli(ctx: click.Context, socket: str | None, output_json: bool) -> None:
    ctx.ensure_object(dict)
    ctx.obj["socket"] = socket
    ctx.obj["json"] = output_json


cli.add_command(status_cmd)
cli.add_command(sync_cmd)
cli.add_command(config_cmd)


def main() -> None:
    cli()
