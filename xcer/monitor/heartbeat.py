"""Heartbeat logic for job state machine processing."""

import logging
from datetime import datetime

from pymongo import MongoClient

from xcer.config import load_clusters, get_preset_for_cluster, build_slurm_args
from xcer.data_types import Job, NextAction, SlurmJobState
from xcer.mongo import jobs as jobs_db
from xcer.remote import slurm
from xcer.utils import get_identity


def process_heartbeat(
    client: MongoClient,
    logger: logging.Logger | None = None,
) -> dict:
    """Process a heartbeat cycle.

    This handles:
    1. Submit jobs with next_action=SUBMIT
    2. Resubmit jobs with next_action=RESUBMIT
    3. Cancel jobs with next_action=CANCEL
    4. Poll active jobs and update states

    Args:
        client: MongoDB client.
        logger: Optional logger.

    Returns:
        Dict with counts of actions taken.
    """
    identity = get_identity(allow_missing=True)
    clusters = load_clusters()
    cluster_map = {c.name: c for c in clusters}

    results = {
        "submitted": 0,
        "resubmitted": 0,
        "cancelled": 0,
        "state_updates": 0,
        "errors": 0,
    }

    # 1. Handle SUBMIT jobs
    submit_jobs = jobs_db.find_jobs_by_next_action(client, NextAction.SUBMIT)
    for job in submit_jobs:
        if job.cluster_name not in cluster_map:
            if logger:
                logger.warning(f"Unknown cluster {job.cluster_name} for job {job.job_name}")
            continue

        try:
            _submit_job(client, job, cluster_map[job.cluster_name], logger)
            results["submitted"] += 1
        except Exception as e:
            if logger:
                logger.error(f"Failed to submit {job.job_name}: {e}")
            results["errors"] += 1

    # 2. Handle RESUBMIT jobs
    resubmit_jobs = jobs_db.find_jobs_by_next_action(client, NextAction.RESUBMIT)
    for job in resubmit_jobs:
        if job.cluster_name not in cluster_map:
            continue

        # Check if under max resubmits
        if job.resubmit_count >= job.max_resubmits:
            if logger:
                logger.info(f"Job {job.job_name} exceeded max resubmits")
            jobs_db.update_job(client, job.job_name, job.cluster_name, {
                "next_action": NextAction.NONE.name,
            })
            continue

        try:
            _submit_job(client, job, cluster_map[job.cluster_name], logger)
            results["resubmitted"] += 1
        except Exception as e:
            if logger:
                logger.error(f"Failed to resubmit {job.job_name}: {e}")
            results["errors"] += 1

    # 3. Handle CANCEL jobs
    cancel_jobs = jobs_db.find_jobs_by_next_action(client, NextAction.CANCEL)
    for job in cancel_jobs:
        if job.cluster_name not in cluster_map:
            continue

        try:
            _cancel_job(client, job, cluster_map[job.cluster_name], logger)
            results["cancelled"] += 1
        except Exception as e:
            if logger:
                logger.error(f"Failed to cancel {job.job_name}: {e}")
            results["errors"] += 1

    # 4. Poll active jobs
    monitor_jobs = jobs_db.find_jobs_by_next_action(client, NextAction.MONITOR)
    for job in monitor_jobs:
        if job.cluster_name not in cluster_map:
            continue

        try:
            updated = _poll_job_state(client, job, cluster_map[job.cluster_name], logger)
            if updated:
                results["state_updates"] += 1
        except Exception as e:
            if logger:
                logger.error(f"Failed to poll {job.job_name}: {e}")
            results["errors"] += 1

    return results


def _submit_job(
    client: MongoClient,
    job: Job,
    cluster,
    logger: logging.Logger | None,
):
    """Submit a job via sbatch."""
    # Check for dependency
    if job.dependency_job_name:
        dep_job = jobs_db.get_job_by_name(client, job.dependency_job_name, job.cluster_name)
        if dep_job and not dep_job.slurm_status.is_terminal():
            if logger:
                logger.debug(f"Job {job.job_name} waiting for dependency {job.dependency_job_name}")
            return  # Wait for dependency

    # Get preset configuration
    preset_config = get_preset_for_cluster(job.preset, job.cluster_name)
    if not preset_config:
        raise ValueError(f"Preset {job.preset} not available on {job.cluster_name}")

    preset, cluster_config = preset_config
    slurm_args = build_slurm_args(preset, cluster_config)

    # Build environment setup for tunnel if needed
    env_setup = ""
    if cluster.requires_tunnel:
        # TODO: Add tunnel setup commands based on cluster config
        pass

    # Submit via sbatch
    result = slurm.sbatch_inline(
        cluster=cluster,
        command=job.command or "echo 'No command specified'",
        job_name=job.job_name,
        slurm_args=slurm_args,
        work_dir=job.work_dir,
        environment_setup=env_setup,
    )

    # Update job in database
    jobs_db.update_job(client, job.job_name, job.cluster_name, {
        "slurm_id": result.slurm_id,
        "next_action": NextAction.MONITOR.name,
        "submitted_at": datetime.utcnow(),
    })

    if logger:
        logger.info(f"Submitted {job.job_name} -> slurm_id={result.slurm_id}")


def _cancel_job(
    client: MongoClient,
    job: Job,
    cluster,
    logger: logging.Logger | None,
):
    """Cancel a job via scancel."""
    if not job.slurm_id:
        # Never submitted, just mark as cancelled
        jobs_db.set_job_ended(client, job.job_name, job.cluster_name, SlurmJobState.CANCELLED)
        return

    success = slurm.scancel(cluster, [job.slurm_id])

    if success:
        jobs_db.set_job_ended(client, job.job_name, job.cluster_name, SlurmJobState.CANCELLED)
        if logger:
            logger.info(f"Cancelled {job.job_name} (slurm_id={job.slurm_id})")
    else:
        if logger:
            logger.warning(f"Failed to cancel {job.job_name} (slurm_id={job.slurm_id})")


def _poll_job_state(
    client: MongoClient,
    job: Job,
    cluster,
    logger: logging.Logger | None,
) -> bool:
    """Poll job state from Slurm and update database.

    Returns True if state changed.
    """
    if not job.slurm_id:
        return False

    new_state = slurm.get_job_state(cluster, job.slurm_id)
    if not new_state:
        return False

    if new_state == job.slurm_status:
        return False  # No change

    # State changed
    updates = {"slurm_status": new_state.value}

    # Track started time
    if new_state == SlurmJobState.RUNNING and job.slurm_status == SlurmJobState.PENDING:
        updates["started_at"] = datetime.utcnow()

    # Handle terminal states
    if new_state.is_terminal():
        updates["ended_at"] = datetime.utcnow()

        # Get exit code from sacct
        acct = slurm.sacct(cluster, job.slurm_id)
        if acct and "exit_code" in acct:
            updates["exit_code"] = acct["exit_code"]

        # Handle failed jobs
        if new_state.is_unexpected() and job.resubmit_on_fail:
            if job.resubmit_count < job.max_resubmits:
                updates["next_action"] = NextAction.RESUBMIT.name
                updates["resubmit_count"] = job.resubmit_count + 1
                if logger:
                    logger.info(f"Job {job.job_name} failed, scheduling resubmit")
            else:
                updates["next_action"] = NextAction.NONE.name
                if logger:
                    logger.info(f"Job {job.job_name} failed, max resubmits reached")
        else:
            updates["next_action"] = NextAction.NONE.name

        if logger:
            logger.info(f"Job {job.job_name} -> {new_state.name}")

    jobs_db.update_job(client, job.job_name, job.cluster_name, updates)
    return True
