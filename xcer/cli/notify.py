"""
Notification commands: show, clear, job, quota.

Manage email notification requests for job and quota conditions.
"""

from typing import Optional

from typing_extensions import Annotated
from pytimeparse import parse as parse_time

import typer

from xcer.mongo import get_mongodb_client
from xcer.services import notify as notify_service
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
    client = get_mongodb_client()
    notifications = notify_service.list_notifications(client, include_disabled=all_)

    if not notifications:
        typer.echo("No notifications configured")
        return

    typer.echo(notify_service.format_notifications_table(notifications))


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
    client = get_mongodb_client()

    if tag:
        deleted = notify_service.delete_notification(client, tag)
        if deleted:
            typer.echo(f"Deleted notification: {tag}")
        else:
            typer.echo(f"Notification not found: {tag}", err=True)
    elif all_:
        notifications = notify_service.list_notifications(client, include_disabled=True)
        count = 0
        for n in notifications:
            if notify_service.delete_notification(client, n.tag):
                count += 1
        typer.echo(f"Deleted {count} notification(s)")
    else:
        typer.echo("Specify --tag or --all", err=True)
        raise typer.Exit(1)


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
    client = get_mongodb_client()
    clusters_list = parse_comma_list(cluster)

    if not any([all_done, any_done, num_done, any_failed, all_failed, num_failed]):
        typer.echo("Specify at least one condition flag (--all-done, --any-failed, etc.)", err=True)
        raise typer.Exit(1)

    if not email:
        typer.echo("Email address required (--email)", err=True)
        raise typer.Exit(1)

    # Generate tag if not provided
    notification_tag = tag or f"job_{id_or_name[0][:20]}"

    # Parse recur interval
    recur_hours = None
    if recur:
        seconds = parse_time(recur)
        if seconds:
            recur_hours = seconds / 3600

    try:
        notif = notify_service.create_job_notification(
            client=client,
            tag=notification_tag,
            email=email,
            job_patterns=id_or_name,
            clusters=clusters_list,
            all_done=all_done,
            any_failed=any_failed,
            any_timeout=False,
            recur_hours=recur_hours,
        )
        typer.echo(f"Created notification: {notif.tag}")
        typer.echo(f"  Email: {notif.email}")
        typer.echo(f"  Patterns: {', '.join(id_or_name)}")
    except notify_service.NotifyError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


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
    client = get_mongodb_client()

    if not email:
        typer.echo("Email address required (--email)", err=True)
        raise typer.Exit(1)

    # Generate tag if not provided
    notification_tag = tag or f"quota_{percent}pct"

    # Parse recur interval
    recur_hours = 24.0
    if recur:
        seconds = parse_time(recur)
        if seconds:
            recur_hours = seconds / 3600

    notif = notify_service.create_quota_notification(
        client=client,
        tag=notification_tag,
        email=email,
        threshold_percent=float(percent),
        recur_hours=recur_hours,
    )
    typer.echo(f"Created notification: {notif.tag}")
    typer.echo(f"  Email: {notif.email}")
    typer.echo(f"  Threshold: {percent}%")
