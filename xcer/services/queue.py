"""Job queue listing service."""

from datetime import datetime, timedelta

from pymongo import MongoClient

from xcer.data_types import Job, SlurmJobState
from xcer.mongo import jobs as jobs_db


def list_jobs(
    client: MongoClient,
    name_pattern: str | None = None,
    cluster_names: list[str] | None = None,
    active_only: bool = False,
    include_recent: timedelta | None = None,
    limit: int = 100,
) -> list[Job]:
    """List jobs matching criteria.

    Args:
        client: MongoDB client.
        name_pattern: Glob pattern for job names (e.g., "train*").
        cluster_names: Filter by clusters.
        active_only: Only show non-terminal jobs.
        include_recent: Include terminal jobs that ended within this time.
        limit: Max number of jobs to return.

    Returns:
        List of matching Job objects, sorted by submission time (newest first).
    """
    since = None
    if include_recent:
        since = datetime.utcnow() - include_recent

    return jobs_db.find_jobs(
        client,
        name_pattern=name_pattern,
        clusters=cluster_names,
        active_only=active_only,
        since=since,
        limit=limit,
    )


def list_active_jobs(
    client: MongoClient,
    cluster_name: str | None = None,
) -> list[Job]:
    """List all non-terminal jobs.

    Args:
        client: MongoDB client.
        cluster_name: Optional cluster filter.

    Returns:
        List of active Job objects.
    """
    return jobs_db.find_jobs(
        client,
        cluster_name=cluster_name,
        active_only=True,
        limit=1000,
    )


def list_pending_jobs(
    client: MongoClient,
    cluster_name: str | None = None,
) -> list[Job]:
    """List jobs waiting to be submitted.

    Args:
        client: MongoDB client.
        cluster_name: Optional cluster filter.

    Returns:
        List of pending Job objects.
    """
    return jobs_db.find_jobs(
        client,
        cluster_name=cluster_name,
        states=[SlurmJobState.PENDING],
        limit=1000,
    )


def get_job_counts(
    client: MongoClient,
    cluster_name: str | None = None,
) -> dict[str, int]:
    """Get job counts by state.

    Args:
        client: MongoDB client.
        cluster_name: Optional cluster filter.

    Returns:
        Dict mapping state name to count.
    """
    return jobs_db.count_jobs_by_state(client, cluster_name)


def format_job_table(jobs: list[Job]) -> str:
    """Format jobs as a table for display.

    Args:
        jobs: List of jobs to format.

    Returns:
        Formatted table string.
    """
    if not jobs:
        return "No jobs found."

    # Column widths
    headers = ["Name", "Cluster", "Status", "Slurm ID", "Preset", "Submitted"]
    rows = []

    for job in jobs:
        submitted = ""
        if job.submitted_at:
            submitted = job.submitted_at.strftime("%Y-%m-%d %H:%M")

        rows.append([
            job.job_name,
            job.cluster_name,
            job.slurm_status.name,
            job.slurm_id or "-",
            job.preset,
            submitted,
        ])

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    # Format header
    header_line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    separator = "  ".join("-" * w for w in widths)

    # Format rows
    row_lines = []
    for row in rows:
        row_lines.append("  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))

    return "\n".join([header_line, separator] + row_lines)


def format_job_detail(job: Job) -> str:
    """Format a single job with full details.

    Args:
        job: Job to format.

    Returns:
        Formatted detail string.
    """
    lines = [
        f"Job: {job.job_name}",
        f"  Cluster: {job.cluster_name}",
        f"  Preset: {job.preset}",
        f"  Status: {job.slurm_status.name}",
        f"  Next Action: {job.next_action.name}",
        f"  Slurm ID: {job.slurm_id or 'Not yet assigned'}",
        f"  Issued By: {job.issued_by}",
    ]

    if job.submitted_at:
        lines.append(f"  Submitted: {job.submitted_at}")
    if job.started_at:
        lines.append(f"  Started: {job.started_at}")
    if job.ended_at:
        lines.append(f"  Ended: {job.ended_at}")
    if job.exit_code is not None:
        lines.append(f"  Exit Code: {job.exit_code}")
    if job.work_dir:
        lines.append(f"  Work Dir: {job.work_dir}")
    if job.command:
        lines.append(f"  Command: {job.command}")
    if job.dependency_job_name:
        lines.append(f"  Depends On: {job.dependency_job_name}")
    if job.resubmit_on_fail:
        lines.append(f"  Resubmit: {job.resubmit_count}/{job.max_resubmits}")

    return "\n".join(lines)
