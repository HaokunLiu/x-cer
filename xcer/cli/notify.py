"""
Notification commands: show, clear, job, quota.

Manage email notification requests for job and quota conditions.
"""

from typing import Optional

from typing_extensions import Annotated

import typer

from .common import Cluster, Preset, parse_comma_list


def show(
    tag: Annotated[Optional[str], typer.Option("-t", "--tag", help="Filter by tag pattern")] = None,
    all_: Annotated[bool, typer.Option("--all", help="Show all notification requests")] = False,
) -> None:
    """
    Show notification requests.

    Examples:
        xcer notify show --all
        xcer notify show --tag "job*"
    """
    from xcer.core.notifications import show_notifications

    show_notifications(tag=tag, show_all=all_)


def clear(
    tag: Annotated[Optional[str], typer.Option("-t", "--tag", help="Clear by tag pattern")] = None,
    all_: Annotated[bool, typer.Option("--all", help="Clear all notification requests")] = False,
) -> None:
    """
    Clear notification requests.

    Examples:
        xcer notify clear --all
        xcer notify clear --tag "job*"
    """
    from xcer.core.notifications import clear_notifications

    clear_notifications(tag=tag, clear_all=all_)


def job(
    id_or_name: Annotated[list[str], typer.Argument(help="Job ID or name patterns")],
    name: Annotated[bool, typer.Option("-n", "--name", help="Treat as job name")] = False,
    id_: Annotated[bool, typer.Option("-i", "--id", help="Treat as job ID")] = False,
    preset: Preset = None,
    cluster: Cluster = None,
    recur: Annotated[str, typer.Option("-r", "--recur", help="Cooldown period (e.g. 1d)")] = "1d",
    email: Annotated[
        Optional[str], typer.Option("-e", "--email", help="Email address for notifications")
    ] = None,
    tag: Annotated[Optional[str], typer.Option("-t", "--tag", help="Tag for this notification")] = None,
    all_done: Annotated[bool, typer.Option("--all-done", help="Notify when all jobs done")] = False,
    any_done: Annotated[bool, typer.Option("--any-done", help="Notify when any job done")] = False,
    num_done: Annotated[
        Optional[int], typer.Option("--num-done", help="Notify when N jobs done")
    ] = None,
    any_failed: Annotated[bool, typer.Option("--any-failed", help="Notify when any job failed")] = False,
    all_failed: Annotated[bool, typer.Option("--all-failed", help="Notify when all jobs failed")] = False,
    num_failed: Annotated[
        Optional[int], typer.Option("--num-failed", help="Notify when N jobs failed")
    ] = None,
) -> None:
    """
    Notify when job conditions are met.

    \b
    Arguments:
        ID_OR_NAME  Job ID or name pattern (e.g. train*, host:123456)

    \b
    Condition flags (at least one required):
        --all-done    Notify when ALL matching jobs complete
        --any-done    Notify when ANY matching job completes
        --num-done N  Notify when N jobs complete
        --any-failed  Notify when ANY job fails
        --all-failed  Notify when ALL jobs fail
        --num-failed N  Notify when N jobs fail

    \b
    Examples:
        # Notify when all training jobs complete
        xcer notify job --all-done train*

        # Notify when any job fails on cluster1
        xcer notify job --any-failed -c cluster1 my_job

        # Notify when 10 batch jobs complete
        xcer notify job --num-done 10 batch_*

        # Combined: all done OR any failed
        xcer notify job --all-done --any-failed train*

        # Filter by preset
        xcer notify job --all-done -p gpu_l40s
    """
    from xcer.core.notifications import notify_on_job

    notify_on_job(
        id_or_name=id_or_name,
        by_name=name,
        by_id=id_,
        preset=preset,
        clusters=parse_comma_list(cluster),
        recur=recur,
        email=email,
        tag=tag,
        all_done=all_done,
        any_done=any_done,
        num_done=num_done,
        any_failed=any_failed,
        all_failed=all_failed,
        num_failed=num_failed,
    )


def quota(
    cluster: Cluster = None,
    percent: Annotated[
        int, typer.Option("-p", "--percent", help="Quota threshold percentage")
    ] = 90,
    recur: Annotated[str, typer.Option("-r", "--recur", help="Cooldown period (e.g. 1d)")] = "1d",
    email: Annotated[
        Optional[str], typer.Option("-e", "--email", help="Email address for notifications")
    ] = None,
    tag: Annotated[Optional[str], typer.Option("-t", "--tag", help="Tag for this notification")] = None,
) -> None:
    """
    Notify when storage quota usage exceeds threshold.

    \b
    Examples:
        # Notify when quota exceeds 90% (default)
        xcer notify quota

        # Custom threshold
        xcer notify quota -p 80

        # Specific clusters only
        xcer notify quota -p 90 -c cluster1,cluster2

        # Check more frequently
        xcer notify quota -p 95 -r 12h
    """
    from xcer.core.notifications import notify_on_quota

    notify_on_quota(
        clusters=parse_comma_list(cluster),
        percent=percent,
        recur=recur,
        email=email,
        tag=tag,
    )
