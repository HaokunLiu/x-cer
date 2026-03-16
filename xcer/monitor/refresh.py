"""Refresh logic for updating cluster stats cache."""

import logging
from datetime import datetime

from pymongo import MongoClient

from xcer.config import load_clusters, load_presets, get_preset_for_cluster
from xcer.data_types import SlurmJobState
from xcer.mongo import stats as stats_db
from xcer.mongo import jobs as jobs_db
from xcer.mongo.stats import ClusterStats
from xcer.remote import slurm


def process_refresh(
    client: MongoClient,
    logger: logging.Logger | None = None,
) -> dict:
    """Process a refresh cycle.

    This handles:
    1. Poll sinfo from each cluster
    2. Update cluster stats cache in MongoDB

    Args:
        client: MongoDB client.
        logger: Optional logger.

    Returns:
        Dict with counts and errors.
    """
    clusters = load_clusters()
    presets = load_presets()

    results = {
        "clusters_updated": 0,
        "stats_updated": 0,
        "errors": 0,
    }

    for cluster in clusters:
        try:
            _update_cluster_stats(client, cluster, presets, logger)
            results["clusters_updated"] += 1
        except Exception as e:
            if logger:
                logger.error(f"Failed to refresh stats for {cluster.name}: {e}")
            results["errors"] += 1

    return results


def _update_cluster_stats(
    client: MongoClient,
    cluster,
    presets,
    logger: logging.Logger | None,
):
    """Update stats for a single cluster."""
    # Get partition info from sinfo
    try:
        partitions = slurm.sinfo(cluster)
    except Exception as e:
        if logger:
            logger.warning(f"Failed to get sinfo from {cluster.name}: {e}")
        partitions = []

    # Build partition lookup
    partition_map = {p.name: p for p in partitions}

    # Get job counts
    try:
        squeue_jobs = slurm.squeue(cluster)
        pending_count = sum(1 for j in squeue_jobs if j.state == SlurmJobState.PENDING)
        running_count = sum(1 for j in squeue_jobs if j.state == SlurmJobState.RUNNING)
    except Exception as e:
        if logger:
            logger.warning(f"Failed to get squeue from {cluster.name}: {e}")
        pending_count = 0
        running_count = 0

    # Update stats for each preset available on this cluster
    updated_count = 0
    for preset in presets:
        preset_config = get_preset_for_cluster(preset.name, cluster.name)
        if not preset_config:
            continue  # Preset not available on this cluster

        _, cluster_config = preset_config

        # Get partition stats if specified
        nodes_total = 0
        nodes_idle = 0
        nodes_allocated = 0

        if cluster_config.partition and cluster_config.partition in partition_map:
            p = partition_map[cluster_config.partition]
            nodes_total = p.nodes_total
            nodes_idle = p.nodes_idle
            nodes_allocated = p.nodes_allocated
        elif partitions:
            # Sum all partitions
            nodes_total = sum(p.nodes_total for p in partitions)
            nodes_idle = sum(p.nodes_idle for p in partitions)
            nodes_allocated = sum(p.nodes_allocated for p in partitions)

        # Calculate load factor
        load_factor = 0.0
        if nodes_total > 0:
            load_factor = nodes_allocated / nodes_total

        stats = ClusterStats(
            cluster_name=cluster.name,
            preset=preset.name,
            nodes_total=nodes_total,
            nodes_idle=nodes_idle,
            nodes_allocated=nodes_allocated,
            pending_jobs=pending_count,
            running_jobs=running_count,
            load_factor=load_factor,
            updated_at=datetime.utcnow(),
        )

        stats_db.update_stats(client, stats)
        updated_count += 1

    if logger:
        logger.debug(f"Updated {updated_count} stats entries for {cluster.name}")


def get_refresh_summary(client: MongoClient) -> dict:
    """Get a summary of current cached stats.

    Returns:
        Dict with overall system statistics.
    """
    clusters = load_clusters()
    cluster_names = [c.name for c in clusters]

    stats_by_cluster = stats_db.get_stats_for_clusters(client, cluster_names)

    total_nodes = 0
    total_idle = 0
    total_allocated = 0
    total_pending = 0
    total_running = 0

    cluster_summaries = {}
    for name, stats_list in stats_by_cluster.items():
        if not stats_list:
            continue

        # Get per-cluster totals (avoid double-counting)
        cluster_nodes = max((s.nodes_total for s in stats_list), default=0)
        cluster_idle = max((s.nodes_idle for s in stats_list), default=0)
        cluster_allocated = max((s.nodes_allocated for s in stats_list), default=0)
        cluster_pending = stats_list[0].pending_jobs if stats_list else 0
        cluster_running = stats_list[0].running_jobs if stats_list else 0

        total_nodes += cluster_nodes
        total_idle += cluster_idle
        total_allocated += cluster_allocated
        total_pending += cluster_pending
        total_running += cluster_running

        cluster_summaries[name] = {
            "nodes": cluster_nodes,
            "idle": cluster_idle,
            "allocated": cluster_allocated,
            "pending_jobs": cluster_pending,
            "running_jobs": cluster_running,
        }

    return {
        "total_nodes": total_nodes,
        "total_idle": total_idle,
        "total_allocated": total_allocated,
        "total_pending": total_pending,
        "total_running": total_running,
        "clusters": cluster_summaries,
    }
