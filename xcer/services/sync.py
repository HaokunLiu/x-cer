"""File synchronization service."""

from pathlib import Path

from xcer.config import load_clusters, load_system_config
from xcer.data_types import ClusterInfo
from xcer.linked_dirs import find_path_via_closest_linked_dir
from xcer.multi_rsync import run_rsync_commands


class SyncError(Exception):
    """Error during file synchronization."""
    pass


def broadcast(
    source_path: str,
    cluster_names: list[str] | None = None,
    rsync_flags: str = "-avz",
    exclude: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Broadcast files from local to multiple clusters.

    Uses linked_dirs to resolve paths that work across clusters.

    Args:
        source_path: Local path to sync.
        cluster_names: Target clusters (None = all).
        rsync_flags: Rsync flags (default: -avz).
        exclude: Additional paths to exclude.
        dry_run: Preview without syncing.

    Returns:
        Dict mapping cluster name to rsync exit code.

    Raises:
        SyncError: If path resolution fails.
    """
    clusters = load_clusters()
    system_config = load_system_config()

    if cluster_names:
        clusters = [c for c in clusters if c.name in cluster_names]

    if not clusters:
        raise SyncError("No clusters to sync to")

    # Resolve source path through linked_dirs
    source = Path(source_path).expanduser().resolve()
    try:
        linked_path = find_path_via_closest_linked_dir(source)
    except ValueError as e:
        raise SyncError(f"Path not in linked directory: {e}")

    # Build exclude args
    excludes = list(system_config.rsync_ignore_list)
    if exclude:
        excludes.extend(exclude)
    exclude_args = " ".join(f"--exclude='{e}'" for e in excludes)

    # Build rsync commands for each cluster
    commands = {}
    for cluster in clusters:
        dest = f"{cluster.user}@{cluster.hostname}:{linked_path}"
        cmd = f"rsync {rsync_flags} {exclude_args} {source}/ {dest}/"

        if dry_run:
            cmd += " --dry-run"

        commands[cluster.name] = cmd

    # Execute in parallel
    return run_rsync_commands(commands, report_results=True)


def gather(
    source_path: str,
    cluster_names: list[str] | None = None,
    rsync_flags: str = "-avz",
    exclude: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Gather files from multiple clusters to local.

    Args:
        source_path: Path to gather (resolved via linked_dirs on remote).
        cluster_names: Source clusters (None = all).
        rsync_flags: Rsync flags (default: -avz).
        exclude: Additional paths to exclude.
        dry_run: Preview without syncing.

    Returns:
        Dict mapping cluster name to rsync exit code.
    """
    clusters = load_clusters()
    system_config = load_system_config()

    if cluster_names:
        clusters = [c for c in clusters if c.name in cluster_names]

    if not clusters:
        raise SyncError("No clusters to sync from")

    # Resolve local destination through linked_dirs
    local_path = Path(source_path).expanduser().resolve()
    try:
        linked_path = find_path_via_closest_linked_dir(local_path)
    except ValueError as e:
        raise SyncError(f"Path not in linked directory: {e}")

    # Build exclude args
    excludes = list(system_config.rsync_ignore_list)
    if exclude:
        excludes.extend(exclude)
    exclude_args = " ".join(f"--exclude='{e}'" for e in excludes)

    # Build rsync commands for each cluster
    commands = {}
    for cluster in clusters:
        src = f"{cluster.user}@{cluster.hostname}:{linked_path}/"
        cmd = f"rsync {rsync_flags} {exclude_args} {src} {local_path}/"

        if dry_run:
            cmd += " --dry-run"

        commands[cluster.name] = cmd

    return run_rsync_commands(commands, report_results=True)


def sync_between_clusters(
    source_cluster: str,
    dest_clusters: list[str],
    path: str,
    rsync_flags: str = "-avz",
    exclude: list[str] | None = None,
) -> dict[str, int]:
    """Sync files from one cluster to others.

    Useful for distributing outputs from one cluster to others.

    Args:
        source_cluster: Cluster to sync from.
        dest_clusters: Clusters to sync to.
        path: Path to sync (resolved via linked_dirs).
        rsync_flags: Rsync flags.
        exclude: Additional excludes.

    Returns:
        Dict mapping destination cluster to rsync exit code.
    """
    clusters = load_clusters()
    system_config = load_system_config()

    src_cluster = None
    dst_clusters = []
    for c in clusters:
        if c.name == source_cluster:
            src_cluster = c
        if c.name in dest_clusters:
            dst_clusters.append(c)

    if not src_cluster:
        raise SyncError(f"Source cluster '{source_cluster}' not found")
    if not dst_clusters:
        raise SyncError("No destination clusters found")

    # Resolve path
    local_path = Path(path).expanduser().resolve()
    try:
        linked_path = find_path_via_closest_linked_dir(local_path)
    except ValueError as e:
        raise SyncError(f"Path not in linked directory: {e}")

    # Build exclude args
    excludes = list(system_config.rsync_ignore_list)
    if exclude:
        excludes.extend(exclude)
    exclude_args = " ".join(f"--exclude='{e}'" for e in excludes)

    # Build rsync commands (running from source cluster)
    source_path = f"{src_cluster.user}@{src_cluster.hostname}:{linked_path}/"
    commands = {}
    for dest in dst_clusters:
        dest_path = f"{dest.user}@{dest.hostname}:{linked_path}/"
        cmd = f"rsync {rsync_flags} {exclude_args} {source_path} {dest_path}"
        commands[f"{source_cluster} -> {dest.name}"] = cmd

    return run_rsync_commands(commands, report_results=True)


def get_sync_status(cluster_names: list[str] | None = None) -> dict[str, bool]:
    """Check if clusters are reachable for sync.

    Args:
        cluster_names: Clusters to check (None = all).

    Returns:
        Dict mapping cluster name to reachability status.
    """
    from xcer.remote.ssh import test_ssh_connection

    clusters = load_clusters()
    if cluster_names:
        clusters = [c for c in clusters if c.name in cluster_names]

    status = {}
    for cluster in clusters:
        status[cluster.name] = test_ssh_connection(cluster, timeout=10)

    return status
