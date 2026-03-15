"""X-CER CLI - Cross Cluster Experiment Remote."""

import sys

import typer

from . import basic, jobs, notify, sync
from xcer.utils import expand_combined_flags

_app = typer.Typer(
    name="xcer",
    help="Cross Cluster Experiment Remote - Multi-cluster job management for HPC",
    no_args_is_help=True,
)

# File sync commands
_app.command()(sync.broadcast)
_app.command()(sync.gather)

# Job management commands
_app.command()(jobs.submit)
_app.command()(jobs.queue)
_app.command()(jobs.cancel)

# Basic commands
_app.command()(basic.info)
_app.command()(basic.monitor)

# Notify subcommands
notify_app = typer.Typer(help="Manage notification requests")
notify_app.command()(notify.show)
notify_app.command()(notify.clear)
notify_app.command()(notify.job)
notify_app.command()(notify.quota)
_app.add_typer(notify_app, name="notify")


def app() -> None:
    """Entry point that preprocesses combined flags like -auvz."""
    sys.argv[1:] = expand_combined_flags(sys.argv[1:])
    _app()


if __name__ == "__main__":
    app()
