"""Cluster and preset information service."""

from datetime import timedelta

from pymongo import MongoClient

from xcer.config import (
    load_clusters,
    load_presets,
    load_environments,
    get_preset_for_cluster,
    build_slurm_args,
)
from xcer.data_types import ClusterInfo, PresetInfo, EnvironmentInfo
from xcer.mongo import stats as stats_db


def get_all_clusters() -> list[ClusterInfo]:
    """Get all configured clusters.

    Returns:
        List of ClusterInfo objects.
    """
    return load_clusters()


def get_all_presets() -> list[PresetInfo]:
    """Get all configured hardware presets.

    Returns:
        List of PresetInfo objects.
    """
    return load_presets()


def get_all_environments() -> list[EnvironmentInfo]:
    """Get all configured environment presets.

    Returns:
        List of EnvironmentInfo objects.
    """
    return load_environments()


def get_cluster_preset_matrix() -> dict[str, dict[str, bool]]:
    """Get matrix of which presets are available on which clusters.

    Returns:
        Dict mapping cluster name to dict of preset name to availability.
    """
    clusters = load_clusters()
    presets = load_presets()

    matrix = {}
    for cluster in clusters:
        matrix[cluster.name] = {}
        for preset in presets:
            result = get_preset_for_cluster(preset.name, cluster.name)
            matrix[cluster.name][preset.name] = result is not None

    return matrix


def get_cluster_info_with_stats(
    client: MongoClient,
    cluster_name: str,
) -> dict:
    """Get cluster info combined with cached stats.

    Args:
        client: MongoDB client.
        cluster_name: Cluster to get info for.

    Returns:
        Dict with cluster config and stats.
    """
    clusters = load_clusters()
    cluster = None
    for c in clusters:
        if c.name == cluster_name:
            cluster = c
            break

    if not cluster:
        return {}

    # Get cached stats
    summary = stats_db.get_cluster_summary(client, cluster_name)

    return {
        "name": cluster.name,
        "hostname": cluster.hostname,
        "user": cluster.user,
        "group_name": cluster.group_name,
        "requires_tunnel": cluster.requires_tunnel,
        "internal_login_node": cluster.internal_login_node,
        "note": cluster.note,
        **summary,
    }


def get_all_info_with_stats(
    client: MongoClient,
    sort_by: str = "name",
) -> list[dict]:
    """Get all clusters with their stats, optionally sorted.

    Args:
        client: MongoDB client.
        sort_by: Sort key - "name", "load", "idle", "jobs".

    Returns:
        List of cluster info dicts.
    """
    clusters = load_clusters()
    results = []

    for cluster in clusters:
        info = get_cluster_info_with_stats(client, cluster.name)
        results.append(info)

    # Sort
    if sort_by == "load":
        # Sort by load factor (lowest first)
        results.sort(key=lambda x: x.get("total_allocated", 0) / max(x.get("total_nodes", 1), 1))
    elif sort_by == "idle":
        # Sort by idle nodes (most first)
        results.sort(key=lambda x: -x.get("total_idle", 0))
    elif sort_by == "jobs":
        # Sort by running jobs (most first)
        results.sort(key=lambda x: -x.get("running_jobs", 0))
    else:
        # Default: sort by name
        results.sort(key=lambda x: x.get("name", ""))

    return results


def format_clusters_table(clusters: list[ClusterInfo]) -> str:
    """Format clusters as a table.

    Args:
        clusters: List of clusters to format.

    Returns:
        Formatted table string.
    """
    if not clusters:
        return "No clusters configured."

    headers = ["Name", "Hostname", "User", "Tunnel", "Note"]
    rows = []

    for c in clusters:
        rows.append([
            c.name,
            c.hostname,
            c.user,
            "Yes" if c.requires_tunnel else "No",
            c.note or "-",
        ])

    return _format_table(headers, rows)


def format_presets_table(presets: list[PresetInfo]) -> str:
    """Format presets as a table.

    Args:
        presets: List of presets to format.

    Returns:
        Formatted table string.
    """
    if not presets:
        return "No presets configured."

    headers = ["Name", "Time", "Memory", "CPUs", "GRES"]
    rows = []

    for p in presets:
        rows.append([
            p.name,
            p.base.time or "-",
            p.base.mem or "-",
            str(p.base.cpus_per_task) if p.base.cpus_per_task else "-",
            p.base.gres or "-",
        ])

    return _format_table(headers, rows)


def format_info_with_stats(info_list: list[dict]) -> str:
    """Format cluster info with stats as a table.

    Args:
        info_list: List of cluster info dicts.

    Returns:
        Formatted table string.
    """
    if not info_list:
        return "No clusters configured."

    headers = ["Cluster", "Nodes", "Idle", "Running", "Pending", "Note"]
    rows = []

    for info in info_list:
        rows.append([
            info.get("name", "-"),
            str(info.get("total_nodes", "-")),
            str(info.get("total_idle", "-")),
            str(info.get("running_jobs", "-")),
            str(info.get("pending_jobs", "-")),
            info.get("note", "-") or "-",
        ])

    return _format_table(headers, rows)


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
