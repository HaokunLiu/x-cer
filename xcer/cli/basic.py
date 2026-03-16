"""
Basic commands: info, monitor.

info - Check cluster status and resource availability
monitor - Control background monitoring daemon
"""

from typing import Literal, Optional

from typing_extensions import Annotated

import typer

from xcer.mongo import get_mongodb_client
from xcer.services import info as info_service
from .common import Cluster, Preset, parse_comma_list


def info(
    cluster: Cluster = None,
    preset: Preset = None,
    sort: Annotated[
        Optional[str],
        typer.Option("-s", "--sort", help="Sort by fields, comma-separated (e.g. load,throughput)"),
    ] = None,
    format_: Annotated[
        Optional[str], typer.Option("-o", "--format", help="Output format specification")
    ] = None,
    refresh: Annotated[bool, typer.Option("--refresh", help="Refresh instead of using cache")] = False,
) -> None:
    """
    Check cluster status and resource availability.

    \b
    Displays for each cluster/preset combination:
        [c]luster   - Cluster name
        [p]reset    - Preset name
        [a]llocated - Allocated GPUs
        [i]dle      - Idle GPUs
        [r]equested - Requested GPUs
        [l]oad      - Load factor: (allocated+requested)/(allocated+idle)
        [t]hroughput- GPU hour throughput

    \b
    Examples:
        # Show all clusters and presets
        xcer info

        # Filter by cluster and preset
        xcer info -c cluster1 -p gpu_l40s

        # Sort by load factor and throughput
        xcer info --sort l,t
        xcer info --sort load,throughput

        # Force refresh (don't use cache)
        xcer info --refresh
    """
    client = get_mongodb_client()

    # Determine sort key
    sort_by = "name"
    if sort:
        sort_fields = parse_comma_list(sort)
        if sort_fields:
            # Map short names to full names
            sort_map = {"l": "load", "i": "idle", "t": "jobs", "j": "jobs"}
            sort_by = sort_map.get(sort_fields[0], sort_fields[0])

    if refresh:
        # Force refresh stats from clusters
        from xcer.monitor import refresh as refresh_module
        refresh_module.process_refresh(client)

    # Get info with stats
    info_list = info_service.get_all_info_with_stats(client, sort_by=sort_by)
    typer.echo(info_service.format_info_with_stats(info_list))


def monitor(
    action: Annotated[
        Literal["start", "stop", "refresh"],
        typer.Argument(help="Action: start, stop, or refresh"),
    ],
) -> None:
    """
    Control background monitoring daemon.

    The daemon runs in the background and periodically:
    - Checks job status (heartbeat cycle)
    - Refreshes cluster info and quota (refresh cycle)
    - Detects conflicts across clusters

    \b
    Commands:
        start   - Start the monitoring daemon
        stop    - Stop the monitoring daemon
        refresh - Force an immediate refresh cycle

    \b
    Examples:
        xcer monitor start
        xcer monitor stop
        xcer monitor refresh
    """
    from xcer.monitor import MonitorBackbone
    from xcer.config import load_system_config

    if action == "start":
        try:
            config = load_system_config()
            m = MonitorBackbone()
            m.start_daemon(config)
        except RuntimeError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(1)
    elif action == "stop":
        MonitorBackbone.end_all_instances()
        typer.echo("Monitor stopped")
    elif action == "refresh":
        MonitorBackbone.request_refresh()
        typer.echo("Refresh requested")
