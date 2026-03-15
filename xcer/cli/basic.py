"""
Basic commands: info, monitor.

info - Check cluster status and resource availability
monitor - Control background monitoring daemon
"""

from typing import Literal, Optional

from typing_extensions import Annotated

import typer

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
    from xcer.core.clusters import get_cluster_info

    get_cluster_info(
        clusters=parse_comma_list(cluster),
        presets=parse_comma_list(preset),
        sort_fields=parse_comma_list(sort),
        format_spec=format_,
        refresh=refresh,
    )


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
    from xcer.monitor import Monitor

    m = Monitor()
    if action == "start":
        m.start()
    elif action == "stop":
        m.stop()
    elif action == "refresh":
        m.refresh()
