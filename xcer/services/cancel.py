"""Job cancellation service."""

from pymongo import MongoClient

from xcer.data_types import Job, NextAction
from xcer.mongo import jobs as jobs_db


class CancelError(Exception):
    """Error during job cancellation."""
    pass


def cancel_jobs(
    client: MongoClient,
    name_pattern: str | None = None,
    cluster_names: list[str] | None = None,
    dry_run: bool = False,
) -> list[Job]:
    """Mark jobs for cancellation.

    Sets next_action=CANCEL on matching jobs. The monitor daemon will
    execute scancel on the target cluster.

    Args:
        client: MongoDB client.
        name_pattern: Glob pattern for job names.
        cluster_names: Filter by clusters.
        dry_run: If True, return jobs that would be cancelled without marking.

    Returns:
        List of jobs that were (or would be) cancelled.

    Raises:
        CancelError: If no jobs match.
    """
    # Find active jobs matching criteria
    jobs = jobs_db.find_jobs(
        client,
        name_pattern=name_pattern,
        clusters=cluster_names,
        active_only=True,
        limit=1000,
    )

    if not jobs:
        raise CancelError("No active jobs found matching criteria")

    if dry_run:
        return jobs

    # Mark each job for cancellation
    cancelled = []
    for job in jobs:
        success = jobs_db.set_job_for_cancel(client, job.job_name, job.cluster_name)
        if success:
            job.next_action = NextAction.CANCEL
            cancelled.append(job)

    return cancelled


def cancel_job_by_name(
    client: MongoClient,
    job_name: str,
    cluster_name: str | None = None,
) -> Job | None:
    """Cancel a specific job by exact name.

    Args:
        client: MongoDB client.
        job_name: Exact job name.
        cluster_name: Optional cluster filter.

    Returns:
        Cancelled Job if found, None otherwise.
    """
    job = jobs_db.get_job_by_name(client, job_name, cluster_name)

    if not job:
        return None

    if not job.slurm_status.is_active():
        return None  # Already terminal

    jobs_db.set_job_for_cancel(client, job.job_name, job.cluster_name)
    job.next_action = NextAction.CANCEL
    return job


def cancel_all_on_cluster(
    client: MongoClient,
    cluster_name: str,
    dry_run: bool = False,
) -> list[Job]:
    """Cancel all active jobs on a specific cluster.

    Args:
        client: MongoDB client.
        cluster_name: Cluster to cancel jobs on.
        dry_run: Preview without cancelling.

    Returns:
        List of cancelled jobs.
    """
    return cancel_jobs(
        client,
        cluster_names=[cluster_name],
        dry_run=dry_run,
    )


def force_cancel_pending(
    client: MongoClient,
    job_name: str,
    cluster_name: str,
) -> bool:
    """Force cancel a job that hasn't been submitted yet.

    For jobs with next_action=SUBMIT that we want to remove before
    the monitor picks them up.

    Args:
        client: MongoDB client.
        job_name: Job to cancel.
        cluster_name: Cluster the job is on.

    Returns:
        True if job was removed.
    """
    job = jobs_db.get_job_by_name(client, job_name, cluster_name)

    if not job:
        return False

    if job.next_action != NextAction.SUBMIT:
        return False  # Already submitted, use normal cancel

    # Delete the job entirely since it was never submitted
    return jobs_db.delete_job(client, job_name, cluster_name)
