"""Notification management service."""

from datetime import datetime, timedelta

from pymongo import MongoClient

from xcer.mongo import notifications as notifs_db
from xcer.mongo.notifications import (
    Notification,
    NotificationType,
    JobCondition,
    QuotaCondition,
)


class NotifyError(Exception):
    """Error managing notifications."""
    pass


def create_job_notification(
    client: MongoClient,
    tag: str,
    email: str,
    job_patterns: list[str],
    clusters: list[str] | None = None,
    all_done: bool = False,
    any_failed: bool = False,
    any_timeout: bool = False,
    recur_hours: float | None = None,
) -> Notification:
    """Create a job-based notification.

    Args:
        client: MongoDB client.
        tag: Unique identifier for this notification.
        email: Email address to notify.
        job_patterns: Glob patterns for job names.
        clusters: Optional cluster filter.
        all_done: Trigger when all matching jobs complete.
        any_failed: Trigger on any job failure.
        any_timeout: Trigger on any timeout.
        recur_hours: Hours between notifications (None = one-shot).

    Returns:
        Created Notification.
    """
    if not any([all_done, any_failed, any_timeout]):
        raise NotifyError("Must specify at least one trigger condition")

    notifs_db.create_job_notification(
        client,
        tag=tag,
        email=email,
        job_patterns=job_patterns,
        clusters=clusters,
        all_done=all_done,
        any_failed=any_failed,
        any_timeout=any_timeout,
        recur_hours=recur_hours,
    )

    return notifs_db.get_notification(client, tag)


def create_quota_notification(
    client: MongoClient,
    tag: str,
    email: str,
    threshold_percent: float = 90.0,
    filesystem: str = "scratch",
    recur_hours: float = 24.0,
) -> Notification:
    """Create a quota-based notification.

    Args:
        client: MongoDB client.
        tag: Unique identifier for this notification.
        email: Email address to notify.
        threshold_percent: Trigger when usage exceeds this percent.
        filesystem: Which filesystem to monitor.
        recur_hours: Hours between notifications.

    Returns:
        Created Notification.
    """
    notifs_db.create_quota_notification(
        client,
        tag=tag,
        email=email,
        threshold_percent=threshold_percent,
        filesystem=filesystem,
        recur_hours=recur_hours,
    )

    return notifs_db.get_notification(client, tag)


def list_notifications(
    client: MongoClient,
    include_disabled: bool = False,
) -> list[Notification]:
    """List all notifications.

    Args:
        client: MongoDB client.
        include_disabled: Include disabled notifications.

    Returns:
        List of Notification objects.
    """
    return notifs_db.get_all_notifications(
        client,
        enabled_only=not include_disabled,
    )


def get_notification(
    client: MongoClient,
    tag: str,
) -> Notification | None:
    """Get a notification by tag.

    Args:
        client: MongoDB client.
        tag: Notification tag.

    Returns:
        Notification if found, None otherwise.
    """
    return notifs_db.get_notification(client, tag)


def enable_notification(
    client: MongoClient,
    tag: str,
) -> bool:
    """Enable a notification.

    Args:
        client: MongoDB client.
        tag: Notification tag.

    Returns:
        True if notification was found and enabled.
    """
    return notifs_db.enable_notification(client, tag)


def disable_notification(
    client: MongoClient,
    tag: str,
) -> bool:
    """Disable a notification.

    Args:
        client: MongoDB client.
        tag: Notification tag.

    Returns:
        True if notification was found and disabled.
    """
    return notifs_db.disable_notification(client, tag)


def delete_notification(
    client: MongoClient,
    tag: str,
) -> bool:
    """Delete a notification.

    Args:
        client: MongoDB client.
        tag: Notification tag.

    Returns:
        True if notification was found and deleted.
    """
    return notifs_db.delete_notification(client, tag)


def format_notifications_table(notifications: list[Notification]) -> str:
    """Format notifications as a table.

    Args:
        notifications: List of notifications.

    Returns:
        Formatted table string.
    """
    if not notifications:
        return "No notifications configured."

    headers = ["Tag", "Type", "Email", "Enabled", "Last Triggered", "Recur"]
    rows = []

    for n in notifications:
        recur = "-"
        if n.recur_interval:
            hours = n.recur_interval.total_seconds() / 3600
            recur = f"{hours:.1f}h"

        last = "-"
        if n.last_triggered:
            last = n.last_triggered.strftime("%Y-%m-%d %H:%M")

        rows.append([
            n.tag,
            n.notification_type.name,
            n.email,
            "Yes" if n.enabled else "No",
            last,
            recur,
        ])

    return _format_table(headers, rows)


def format_notification_detail(n: Notification) -> str:
    """Format a notification with full details.

    Args:
        n: Notification to format.

    Returns:
        Formatted detail string.
    """
    lines = [
        f"Notification: {n.tag}",
        f"  Type: {n.notification_type.name}",
        f"  Email: {n.email}",
        f"  Enabled: {n.enabled}",
        f"  Created: {n.created_at}",
    ]

    if n.last_triggered:
        lines.append(f"  Last Triggered: {n.last_triggered}")

    if n.recur_interval:
        hours = n.recur_interval.total_seconds() / 3600
        lines.append(f"  Recur Every: {hours:.1f} hours")
    else:
        lines.append("  Recur: One-shot")

    if n.job_condition:
        jc = n.job_condition
        lines.append("  Job Condition:")
        lines.append(f"    Patterns: {', '.join(jc.job_patterns)}")
        if jc.clusters:
            lines.append(f"    Clusters: {', '.join(jc.clusters)}")
        triggers = []
        if jc.all_done:
            triggers.append("all_done")
        if jc.any_failed:
            triggers.append("any_failed")
        if jc.any_timeout:
            triggers.append("any_timeout")
        lines.append(f"    Triggers: {', '.join(triggers)}")

    if n.quota_condition:
        qc = n.quota_condition
        lines.append("  Quota Condition:")
        lines.append(f"    Threshold: {qc.threshold_percent}%")
        lines.append(f"    Filesystem: {qc.filesystem}")

    return "\n".join(lines)


def _format_table(headers: list[str], rows: list[list[str]]) -> str:
    """Helper to format a table."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    header_line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    separator = "  ".join("-" * w for w in widths)
    row_lines = [
        "  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row))
        for row in rows
    ]

    return "\n".join([header_line, separator] + row_lines)
