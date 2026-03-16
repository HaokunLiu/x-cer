"""Job CRUD operations in MongoDB."""

import fnmatch
import re
from datetime import datetime, timedelta
from typing import Iterator

from pymongo import MongoClient
from pymongo.collection import Collection

from xcer.data_types import Job, NextAction, SlurmJobState


DB_NAME = "xcer"
COLLECTION_NAME = "jobs"


def _get_collection(client: MongoClient) -> Collection:
    """Get the jobs collection."""
    return client[DB_NAME][COLLECTION_NAME]


def _job_to_doc(job: Job) -> dict:
    """Convert Job dataclass to MongoDB document."""
    return {
        "job_name": job.job_name,
        "preset": job.preset,
        "cluster_name": job.cluster_name,
        "issued_by": job.issued_by,
        "slurm_status": job.slurm_status.value,
        "next_action": job.next_action.name,
        "slurm_id": job.slurm_id,
        "submitted_at": job.submitted_at,
        "started_at": job.started_at,
        "ended_at": job.ended_at,
        "exit_code": job.exit_code,
        "resubmit_on_fail": job.resubmit_on_fail,
        "max_resubmits": job.max_resubmits,
        "resubmit_count": job.resubmit_count,
        "dependency_job_name": job.dependency_job_name,
        "work_dir": job.work_dir,
        "command": job.command,
    }


def _doc_to_job(doc: dict) -> Job:
    """Convert MongoDB document to Job dataclass."""
    return Job(
        job_name=doc["job_name"],
        preset=doc["preset"],
        cluster_name=doc["cluster_name"],
        issued_by=doc["issued_by"],
        slurm_status=SlurmJobState(doc["slurm_status"]),
        next_action=NextAction[doc["next_action"]],
        slurm_id=doc.get("slurm_id"),
        submitted_at=doc.get("submitted_at"),
        started_at=doc.get("started_at"),
        ended_at=doc.get("ended_at"),
        exit_code=doc.get("exit_code"),
        resubmit_on_fail=doc.get("resubmit_on_fail", False),
        max_resubmits=doc.get("max_resubmits", 0),
        resubmit_count=doc.get("resubmit_count", 0),
        dependency_job_name=doc.get("dependency_job_name"),
        work_dir=doc.get("work_dir"),
        command=doc.get("command"),
    )


def create_job(client: MongoClient, job: Job) -> str:
    """Create a new job in the database.

    Args:
        client: MongoDB client.
        job: Job to create.

    Returns:
        Inserted document ID as string.
    """
    collection = _get_collection(client)
    doc = _job_to_doc(job)
    result = collection.insert_one(doc)
    return str(result.inserted_id)


def get_job_by_name(
    client: MongoClient,
    job_name: str,
    cluster_name: str | None = None,
) -> Job | None:
    """Get a job by exact name.

    Args:
        client: MongoDB client.
        job_name: Exact job name.
        cluster_name: Optional cluster filter.

    Returns:
        Job if found, None otherwise.
    """
    collection = _get_collection(client)
    query = {"job_name": job_name}
    if cluster_name:
        query["cluster_name"] = cluster_name

    doc = collection.find_one(query)
    return _doc_to_job(doc) if doc else None


def get_job_by_slurm_id(
    client: MongoClient,
    slurm_id: str,
    cluster_name: str,
) -> Job | None:
    """Get a job by Slurm ID.

    Args:
        client: MongoDB client.
        slurm_id: Slurm job ID.
        cluster_name: Cluster the job is on.

    Returns:
        Job if found, None otherwise.
    """
    collection = _get_collection(client)
    query = {"slurm_id": slurm_id, "cluster_name": cluster_name}
    doc = collection.find_one(query)
    return _doc_to_job(doc) if doc else None


def find_jobs(
    client: MongoClient,
    name_pattern: str | None = None,
    cluster_name: str | None = None,
    clusters: list[str] | None = None,
    states: list[SlurmJobState] | None = None,
    next_actions: list[NextAction] | None = None,
    active_only: bool = False,
    since: datetime | None = None,
    limit: int = 100,
) -> list[Job]:
    """Find jobs matching criteria.

    Args:
        client: MongoDB client.
        name_pattern: Glob pattern for job name (e.g., "train*").
        cluster_name: Single cluster filter.
        clusters: List of clusters to filter.
        states: List of Slurm states to include.
        next_actions: List of NextAction values to include.
        active_only: Only return non-terminal jobs.
        since: Only jobs submitted after this time.
        limit: Max results.

    Returns:
        List of matching Job objects.
    """
    collection = _get_collection(client)
    query: dict = {}

    if name_pattern:
        # Convert glob to regex
        regex = fnmatch.translate(name_pattern)
        query["job_name"] = {"$regex": regex, "$options": "i"}

    if cluster_name:
        query["cluster_name"] = cluster_name
    elif clusters:
        query["cluster_name"] = {"$in": clusters}

    if states:
        query["slurm_status"] = {"$in": [s.value for s in states]}

    if next_actions:
        query["next_action"] = {"$in": [a.name for a in next_actions]}

    if active_only:
        active_states = [s.value for s in SlurmJobState if s.is_active()]
        query["slurm_status"] = {"$in": active_states}

    if since:
        query["submitted_at"] = {"$gte": since}

    cursor = collection.find(query).sort("submitted_at", -1).limit(limit)
    return [_doc_to_job(doc) for doc in cursor]


def find_jobs_by_next_action(
    client: MongoClient,
    next_action: NextAction,
    cluster_name: str | None = None,
) -> list[Job]:
    """Find all jobs with a specific next_action.

    Args:
        client: MongoDB client.
        next_action: NextAction to filter by.
        cluster_name: Optional cluster filter.

    Returns:
        List of matching Job objects.
    """
    collection = _get_collection(client)
    query = {"next_action": next_action.name}
    if cluster_name:
        query["cluster_name"] = cluster_name

    cursor = collection.find(query)
    return [_doc_to_job(doc) for doc in cursor]


def update_job(
    client: MongoClient,
    job_name: str,
    cluster_name: str,
    updates: dict,
) -> bool:
    """Update a job's fields.

    Args:
        client: MongoDB client.
        job_name: Job to update.
        cluster_name: Cluster the job is on.
        updates: Dict of fields to update.

    Returns:
        True if job was found and updated.
    """
    collection = _get_collection(client)
    result = collection.update_one(
        {"job_name": job_name, "cluster_name": cluster_name},
        {"$set": updates},
    )
    return result.modified_count > 0


def update_job_state(
    client: MongoClient,
    job_name: str,
    cluster_name: str,
    slurm_status: SlurmJobState | None = None,
    next_action: NextAction | None = None,
    slurm_id: str | None = None,
    exit_code: int | None = None,
) -> bool:
    """Update job state fields.

    Args:
        client: MongoDB client.
        job_name: Job to update.
        cluster_name: Cluster the job is on.
        slurm_status: New Slurm status.
        next_action: New next action.
        slurm_id: Slurm job ID (if just assigned).
        exit_code: Exit code (if completed).

    Returns:
        True if job was found and updated.
    """
    updates = {}
    if slurm_status is not None:
        updates["slurm_status"] = slurm_status.value
    if next_action is not None:
        updates["next_action"] = next_action.name
    if slurm_id is not None:
        updates["slurm_id"] = slurm_id
    if exit_code is not None:
        updates["exit_code"] = exit_code

    if not updates:
        return False

    return update_job(client, job_name, cluster_name, updates)


def set_job_started(
    client: MongoClient,
    job_name: str,
    cluster_name: str,
) -> bool:
    """Mark a job as started (RUNNING state)."""
    return update_job(
        client,
        job_name,
        cluster_name,
        {
            "slurm_status": SlurmJobState.RUNNING.value,
            "started_at": datetime.utcnow(),
        },
    )


def set_job_ended(
    client: MongoClient,
    job_name: str,
    cluster_name: str,
    slurm_status: SlurmJobState,
    exit_code: int | None = None,
) -> bool:
    """Mark a job as ended (terminal state)."""
    updates = {
        "slurm_status": slurm_status.value,
        "ended_at": datetime.utcnow(),
        "next_action": NextAction.NONE.name,
    }
    if exit_code is not None:
        updates["exit_code"] = exit_code

    return update_job(client, job_name, cluster_name, updates)


def set_job_for_cancel(
    client: MongoClient,
    job_name: str,
    cluster_name: str,
) -> bool:
    """Mark a job for cancellation."""
    return update_job(
        client,
        job_name,
        cluster_name,
        {"next_action": NextAction.CANCEL.name},
    )


def increment_resubmit_count(
    client: MongoClient,
    job_name: str,
    cluster_name: str,
) -> bool:
    """Increment resubmit counter and set next_action to RESUBMIT."""
    collection = _get_collection(client)
    result = collection.update_one(
        {"job_name": job_name, "cluster_name": cluster_name},
        {
            "$inc": {"resubmit_count": 1},
            "$set": {"next_action": NextAction.RESUBMIT.name},
        },
    )
    return result.modified_count > 0


def delete_job(
    client: MongoClient,
    job_name: str,
    cluster_name: str,
) -> bool:
    """Delete a job from the database."""
    collection = _get_collection(client)
    result = collection.delete_one(
        {"job_name": job_name, "cluster_name": cluster_name}
    )
    return result.deleted_count > 0


def delete_old_jobs(
    client: MongoClient,
    older_than: timedelta,
) -> int:
    """Delete terminal jobs older than specified time.

    Args:
        client: MongoDB client.
        older_than: Delete jobs ended before now - older_than.

    Returns:
        Number of jobs deleted.
    """
    collection = _get_collection(client)
    cutoff = datetime.utcnow() - older_than

    terminal_states = [s.value for s in SlurmJobState if s.is_terminal()]
    result = collection.delete_many({
        "slurm_status": {"$in": terminal_states},
        "ended_at": {"$lt": cutoff},
    })
    return result.deleted_count


def count_jobs_by_state(
    client: MongoClient,
    cluster_name: str | None = None,
) -> dict[str, int]:
    """Count jobs grouped by state.

    Returns:
        Dict mapping state value to count.
    """
    collection = _get_collection(client)
    pipeline = []

    if cluster_name:
        pipeline.append({"$match": {"cluster_name": cluster_name}})

    pipeline.append({
        "$group": {
            "_id": "$slurm_status",
            "count": {"$sum": 1},
        }
    })

    result = collection.aggregate(pipeline)
    return {doc["_id"]: doc["count"] for doc in result}
