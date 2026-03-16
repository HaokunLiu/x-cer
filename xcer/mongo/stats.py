"""Cluster stats cache in MongoDB."""

from dataclasses import dataclass
from datetime import datetime, timedelta

from pymongo import MongoClient
from pymongo.collection import Collection


DB_NAME = "xcer"
COLLECTION_NAME = "cluster_stats"


@dataclass
class ClusterStats:
    """Cached statistics for a cluster/preset combination."""
    cluster_name: str
    preset: str
    nodes_total: int
    nodes_idle: int
    nodes_allocated: int
    pending_jobs: int
    running_jobs: int
    load_factor: float  # allocated / total
    updated_at: datetime


def _get_collection(client: MongoClient) -> Collection:
    """Get the cluster_stats collection."""
    return client[DB_NAME][COLLECTION_NAME]


def _stats_to_doc(stats: ClusterStats) -> dict:
    """Convert ClusterStats to MongoDB document."""
    return {
        "_id": f"{stats.cluster_name}:{stats.preset}",
        "cluster_name": stats.cluster_name,
        "preset": stats.preset,
        "nodes_total": stats.nodes_total,
        "nodes_idle": stats.nodes_idle,
        "nodes_allocated": stats.nodes_allocated,
        "pending_jobs": stats.pending_jobs,
        "running_jobs": stats.running_jobs,
        "load_factor": stats.load_factor,
        "updated_at": stats.updated_at,
    }


def _doc_to_stats(doc: dict) -> ClusterStats:
    """Convert MongoDB document to ClusterStats."""
    return ClusterStats(
        cluster_name=doc["cluster_name"],
        preset=doc["preset"],
        nodes_total=doc["nodes_total"],
        nodes_idle=doc["nodes_idle"],
        nodes_allocated=doc["nodes_allocated"],
        pending_jobs=doc["pending_jobs"],
        running_jobs=doc["running_jobs"],
        load_factor=doc["load_factor"],
        updated_at=doc["updated_at"],
    )


def update_stats(client: MongoClient, stats: ClusterStats) -> None:
    """Update or insert cluster stats.

    Args:
        client: MongoDB client.
        stats: Stats to upsert.
    """
    collection = _get_collection(client)
    doc = _stats_to_doc(stats)
    collection.replace_one(
        {"_id": doc["_id"]},
        doc,
        upsert=True,
    )


def get_stats(
    client: MongoClient,
    cluster_name: str,
    preset: str,
) -> ClusterStats | None:
    """Get cached stats for a cluster/preset.

    Args:
        client: MongoDB client.
        cluster_name: Cluster name.
        preset: Preset name.

    Returns:
        ClusterStats if found, None otherwise.
    """
    collection = _get_collection(client)
    doc = collection.find_one({"_id": f"{cluster_name}:{preset}"})
    return _doc_to_stats(doc) if doc else None


def get_all_stats(
    client: MongoClient,
    cluster_name: str | None = None,
    preset: str | None = None,
    max_age: timedelta | None = None,
) -> list[ClusterStats]:
    """Get all cached stats, optionally filtered.

    Args:
        client: MongoDB client.
        cluster_name: Optional cluster filter.
        preset: Optional preset filter.
        max_age: Only return stats updated within this time.

    Returns:
        List of ClusterStats.
    """
    collection = _get_collection(client)
    query: dict = {}

    if cluster_name:
        query["cluster_name"] = cluster_name
    if preset:
        query["preset"] = preset
    if max_age:
        cutoff = datetime.utcnow() - max_age
        query["updated_at"] = {"$gte": cutoff}

    cursor = collection.find(query)
    return [_doc_to_stats(doc) for doc in cursor]


def get_stats_for_clusters(
    client: MongoClient,
    cluster_names: list[str],
    preset: str | None = None,
) -> dict[str, list[ClusterStats]]:
    """Get stats grouped by cluster.

    Args:
        client: MongoDB client.
        cluster_names: Clusters to query.
        preset: Optional preset filter.

    Returns:
        Dict mapping cluster name to list of stats.
    """
    collection = _get_collection(client)
    query = {"cluster_name": {"$in": cluster_names}}
    if preset:
        query["preset"] = preset

    cursor = collection.find(query)
    result: dict[str, list[ClusterStats]] = {name: [] for name in cluster_names}
    for doc in cursor:
        stats = _doc_to_stats(doc)
        result[stats.cluster_name].append(stats)

    return result


def find_best_cluster(
    client: MongoClient,
    preset: str,
    cluster_names: list[str],
    strategy: str = "load",
) -> str | None:
    """Find the best cluster for a preset based on strategy.

    Args:
        client: MongoDB client.
        preset: Hardware preset to check.
        cluster_names: Clusters to consider.
        strategy: Selection strategy - "load" (lowest load_factor),
                  "idle" (most idle nodes), "throughput" (most running jobs).

    Returns:
        Best cluster name, or None if no stats available.
    """
    stats_list = get_all_stats(client, preset=preset)
    stats_list = [s for s in stats_list if s.cluster_name in cluster_names]

    if not stats_list:
        return None

    if strategy == "load":
        # Lowest load factor
        best = min(stats_list, key=lambda s: s.load_factor)
    elif strategy == "idle":
        # Most idle nodes
        best = max(stats_list, key=lambda s: s.nodes_idle)
    elif strategy == "throughput":
        # Most currently running (cluster is handling jobs well)
        best = max(stats_list, key=lambda s: s.running_jobs)
    else:
        best = stats_list[0]

    return best.cluster_name


def delete_old_stats(
    client: MongoClient,
    older_than: timedelta,
) -> int:
    """Delete stats older than specified time.

    Args:
        client: MongoDB client.
        older_than: Delete stats not updated in this duration.

    Returns:
        Number of documents deleted.
    """
    collection = _get_collection(client)
    cutoff = datetime.utcnow() - older_than
    result = collection.delete_many({"updated_at": {"$lt": cutoff}})
    return result.deleted_count


def get_cluster_summary(
    client: MongoClient,
    cluster_name: str,
) -> dict:
    """Get summary statistics for a cluster across all presets.

    Returns:
        Dict with total_nodes, total_idle, total_allocated, total_jobs, etc.
    """
    stats_list = get_all_stats(client, cluster_name=cluster_name)

    if not stats_list:
        return {}

    return {
        "cluster_name": cluster_name,
        "total_nodes": sum(s.nodes_total for s in stats_list),
        "total_idle": sum(s.nodes_idle for s in stats_list),
        "total_allocated": sum(s.nodes_allocated for s in stats_list),
        "pending_jobs": sum(s.pending_jobs for s in stats_list),
        "running_jobs": sum(s.running_jobs for s in stats_list),
        "presets": [s.preset for s in stats_list],
        "last_updated": max(s.updated_at for s in stats_list),
    }
