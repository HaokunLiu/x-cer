"""Job submission service."""

from datetime import datetime

from pymongo import MongoClient

from xcer.config import (
    get_preset_for_cluster,
    build_slurm_args,
    load_clusters,
    load_presets,
)
from xcer.data_types import Job, NextAction, SlurmJobState
from xcer.mongo import jobs as jobs_db
from xcer.mongo import stats as stats_db
from xcer.utils import get_identity


class SubmitError(Exception):
    """Error during job submission."""
    pass


def submit_job(
    client: MongoClient,
    job_name: str,
    preset_name: str,
    command: str,
    work_dir: str | None = None,
    cluster_names: list[str] | None = None,
    dependency: str | None = None,
    resubmit_on_fail: bool = False,
    max_resubmits: int = 0,
    strategy: str = "load",
) -> Job:
    """Submit a job to be scheduled by the monitor daemon.

    This creates a job record with next_action=SUBMIT. The monitor daemon
    will pick it up and actually run sbatch on the target cluster.

    Args:
        client: MongoDB client.
        job_name: Unique name for the job.
        preset_name: Hardware preset to use.
        command: Command to execute.
        work_dir: Working directory (resolved via linked_dirs).
        cluster_names: Target clusters (None = select best automatically).
        dependency: Name of job this depends on.
        resubmit_on_fail: Whether to resubmit on failure.
        max_resubmits: Max resubmit attempts.
        strategy: Cluster selection strategy ("load", "idle", "throughput").

    Returns:
        Created Job object.

    Raises:
        SubmitError: If submission fails (invalid preset, no clusters, etc).
    """
    # Load config
    all_clusters = load_clusters()
    all_presets = load_presets()

    # Validate preset exists
    preset = None
    for p in all_presets:
        if p.name == preset_name:
            preset = p
            break

    if not preset:
        available = [p.name for p in all_presets]
        raise SubmitError(f"Preset '{preset_name}' not found. Available: {available}")

    # Determine target cluster(s)
    if cluster_names:
        # Validate specified clusters
        cluster_name_set = set(c.name for c in all_clusters)
        for name in cluster_names:
            if name not in cluster_name_set:
                raise SubmitError(f"Cluster '{name}' not found")
    else:
        # Get clusters where preset is available
        cluster_names = []
        for cluster in all_clusters:
            result = get_preset_for_cluster(preset_name, cluster.name)
            if result:  # Preset available on this cluster
                cluster_names.append(cluster.name)

        if not cluster_names:
            raise SubmitError(f"Preset '{preset_name}' not available on any cluster")

    # Select best cluster using cached stats
    if len(cluster_names) == 1:
        target_cluster = cluster_names[0]
    else:
        target_cluster = stats_db.find_best_cluster(
            client, preset_name, cluster_names, strategy=strategy
        )
        if not target_cluster:
            # No stats available, use first cluster
            target_cluster = cluster_names[0]

    # Check for existing job with same name
    existing = jobs_db.get_job_by_name(client, job_name, target_cluster)
    if existing:
        if existing.slurm_status.is_active():
            raise SubmitError(
                f"Job '{job_name}' already exists on {target_cluster} "
                f"(status: {existing.slurm_status.name})"
            )
        # If terminal, delete old record
        jobs_db.delete_job(client, job_name, target_cluster)

    # Create job record
    job = Job(
        job_name=job_name,
        preset=preset_name,
        cluster_name=target_cluster,
        issued_by=get_identity(allow_missing=True),
        slurm_status=SlurmJobState.PENDING,
        next_action=NextAction.SUBMIT,
        submitted_at=datetime.utcnow(),
        resubmit_on_fail=resubmit_on_fail,
        max_resubmits=max_resubmits,
        dependency_job_name=dependency,
        work_dir=work_dir,
        command=command,
    )

    jobs_db.create_job(client, job)
    return job


def submit_to_multiple_clusters(
    client: MongoClient,
    job_name: str,
    preset_name: str,
    command: str,
    cluster_names: list[str],
    work_dir: str | None = None,
) -> list[Job]:
    """Submit the same job to multiple clusters.

    Creates separate job records for each cluster with numbered suffixes.

    Args:
        client: MongoDB client.
        job_name: Base job name (will have cluster suffix added).
        preset_name: Hardware preset to use.
        command: Command to execute.
        cluster_names: Target clusters.
        work_dir: Working directory.

    Returns:
        List of created Job objects.
    """
    jobs = []
    for cluster in cluster_names:
        full_name = f"{job_name}_{cluster}"
        job = submit_job(
            client,
            job_name=full_name,
            preset_name=preset_name,
            command=command,
            work_dir=work_dir,
            cluster_names=[cluster],
        )
        jobs.append(job)

    return jobs


def get_available_presets_for_clusters(
    cluster_names: list[str] | None = None,
) -> dict[str, list[str]]:
    """Get presets available on each cluster.

    Args:
        cluster_names: Clusters to check (None = all).

    Returns:
        Dict mapping cluster name to list of available preset names.
    """
    all_clusters = load_clusters()
    all_presets = load_presets()

    if cluster_names:
        clusters = [c for c in all_clusters if c.name in cluster_names]
    else:
        clusters = all_clusters

    result = {}
    for cluster in clusters:
        available = []
        for preset in all_presets:
            config = get_preset_for_cluster(preset.name, cluster.name)
            if config:
                available.append(preset.name)
        result[cluster.name] = available

    return result
