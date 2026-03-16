"""Alert processing for notifications."""

import fnmatch
import logging
import subprocess
from datetime import datetime

from pymongo import MongoClient

from xcer.config import load_clusters
from xcer.data_types import SlurmJobState
from xcer.mongo import jobs as jobs_db
from xcer.mongo import notifications as notifs_db
from xcer.mongo.notifications import Notification, NotificationType
from xcer.remote.ssh import run_ssh_command


def process_alerts(
    client: MongoClient,
    logger: logging.Logger | None = None,
) -> dict:
    """Process due alerts and send notifications.

    Args:
        client: MongoDB client.
        logger: Optional logger.

    Returns:
        Dict with counts of alerts processed.
    """
    results = {
        "checked": 0,
        "triggered": 0,
        "errors": 0,
    }

    due_notifications = notifs_db.get_due_notifications(client)

    for notif in due_notifications:
        results["checked"] += 1

        try:
            triggered = _check_and_trigger(client, notif, logger)
            if triggered:
                results["triggered"] += 1
        except Exception as e:
            if logger:
                logger.error(f"Failed to process alert {notif.tag}: {e}")
            results["errors"] += 1

    return results


def _check_and_trigger(
    client: MongoClient,
    notif: Notification,
    logger: logging.Logger | None,
) -> bool:
    """Check if notification should trigger and send if so.

    Returns True if triggered.
    """
    should_trigger = False
    message = ""

    if notif.notification_type == NotificationType.JOB:
        should_trigger, message = _check_job_condition(client, notif)
    elif notif.notification_type == NotificationType.QUOTA:
        should_trigger, message = _check_quota_condition(notif)
    elif notif.notification_type == NotificationType.CUSTOM:
        should_trigger, message = _check_custom_condition(notif)

    if should_trigger:
        _send_notification(notif, message, logger)
        notifs_db.mark_triggered(client, notif.tag)

        # Disable one-shot notifications
        if not notif.recur_interval:
            notifs_db.disable_notification(client, notif.tag)

        return True

    return False


def _check_job_condition(
    client: MongoClient,
    notif: Notification,
) -> tuple[bool, str]:
    """Check job-based notification conditions.

    Returns (should_trigger, message).
    """
    if not notif.job_condition:
        return False, ""

    jc = notif.job_condition

    # Find matching jobs
    matching_jobs = []
    for pattern in jc.job_patterns:
        jobs = jobs_db.find_jobs(
            client,
            name_pattern=pattern,
            clusters=jc.clusters if jc.clusters else None,
            limit=1000,
        )
        matching_jobs.extend(jobs)

    # Remove duplicates
    seen = set()
    unique_jobs = []
    for job in matching_jobs:
        key = (job.job_name, job.cluster_name)
        if key not in seen:
            seen.add(key)
            unique_jobs.append(job)

    if not unique_jobs:
        return False, ""

    # Check conditions
    if jc.all_done:
        all_terminal = all(j.slurm_status.is_terminal() for j in unique_jobs)
        if all_terminal:
            completed = sum(1 for j in unique_jobs if j.slurm_status.is_successful())
            failed = len(unique_jobs) - completed
            return True, f"All {len(unique_jobs)} jobs complete ({completed} succeeded, {failed} failed)"

    if jc.any_failed:
        failed_jobs = [j for j in unique_jobs if j.slurm_status == SlurmJobState.FAILED]
        if failed_jobs:
            names = [j.job_name for j in failed_jobs[:3]]
            more = len(failed_jobs) - 3
            extra = f" (+{more} more)" if more > 0 else ""
            return True, f"Job(s) failed: {', '.join(names)}{extra}"

    if jc.any_timeout:
        timeout_jobs = [j for j in unique_jobs if j.slurm_status == SlurmJobState.TIMEOUT]
        if timeout_jobs:
            names = [j.job_name for j in timeout_jobs[:3]]
            more = len(timeout_jobs) - 3
            extra = f" (+{more} more)" if more > 0 else ""
            return True, f"Job(s) timed out: {', '.join(names)}{extra}"

    return False, ""


def _check_quota_condition(notif: Notification) -> tuple[bool, str]:
    """Check quota-based notification conditions.

    Returns (should_trigger, message).
    """
    if not notif.quota_condition:
        return False, ""

    qc = notif.quota_condition

    # Get quota info from first available cluster
    # TODO: This should be configurable per cluster
    clusters = load_clusters()
    if not clusters:
        return False, ""

    cluster = clusters[0]

    try:
        # Run quota command
        result = run_ssh_command(cluster, f"quota -s 2>/dev/null || df -h ~")
        output = result.stdout

        # Parse quota output (simplified - real implementation needs cluster-specific parsing)
        # Looking for patterns like "50G/100G" or "50%" etc.
        usage_percent = _parse_quota_output(output, qc.filesystem)

        if usage_percent and usage_percent >= qc.threshold_percent:
            return True, f"Quota usage at {usage_percent:.1f}% (threshold: {qc.threshold_percent}%)"

    except Exception:
        pass

    return False, ""


def _parse_quota_output(output: str, filesystem: str) -> float | None:
    """Parse quota command output to get usage percentage."""
    # This is a simplified parser - real implementation would be more robust
    lines = output.strip().split("\n")

    for line in lines:
        if filesystem in line.lower():
            # Try to find percentage
            parts = line.split()
            for part in parts:
                if part.endswith("%"):
                    try:
                        return float(part[:-1])
                    except ValueError:
                        pass

            # Try to find used/total and calculate
            for i, part in enumerate(parts):
                if "/" in part and i > 0:
                    try:
                        # Pattern like "50G/100G"
                        used, total = part.split("/")
                        used_val = _parse_size(used)
                        total_val = _parse_size(total)
                        if used_val and total_val and total_val > 0:
                            return (used_val / total_val) * 100
                    except (ValueError, IndexError):
                        pass

    return None


def _parse_size(size_str: str) -> float | None:
    """Parse size string like '50G' to bytes."""
    multipliers = {"K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15}

    size_str = size_str.strip().upper()
    if not size_str:
        return None

    for suffix, mult in multipliers.items():
        if size_str.endswith(suffix):
            try:
                return float(size_str[:-1]) * mult
            except ValueError:
                return None

    try:
        return float(size_str)
    except ValueError:
        return None


def _check_custom_condition(notif: Notification) -> tuple[bool, str]:
    """Check custom command-based notification conditions.

    Returns (should_trigger, message).
    """
    if not notif.custom_command:
        return False, ""

    # TODO: Implement custom command execution
    # This would run a user-defined script and check exit code
    return False, ""


def _send_notification(
    notif: Notification,
    message: str,
    logger: logging.Logger | None,
):
    """Send notification via email using cluster's mail command."""
    subject = f"[X-CER] {notif.tag}: {notif.notification_type.name}"
    body = f"""X-CER Notification

Tag: {notif.tag}
Type: {notif.notification_type.name}
Time: {datetime.utcnow().isoformat()}

{message}
"""

    # Use cluster mail command
    clusters = load_clusters()
    if not clusters:
        if logger:
            logger.warning(f"No clusters available to send notification {notif.tag}")
        return

    cluster = clusters[0]

    try:
        # Send email via mail command
        mail_cmd = f'echo "{body}" | mail -s "{subject}" {notif.email}'
        result = run_ssh_command(cluster, mail_cmd)

        if result.exit_code == 0:
            if logger:
                logger.info(f"Sent notification {notif.tag} to {notif.email}")
        else:
            if logger:
                logger.warning(f"Failed to send notification {notif.tag}: {result.stdout}")

    except Exception as e:
        if logger:
            logger.error(f"Error sending notification {notif.tag}: {e}")
