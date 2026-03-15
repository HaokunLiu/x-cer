"""Cluster status and information."""

from typing import Optional


def get_cluster_info(
    clusters: list[str],
    presets: list[str],
    sort_fields: list[str],
    format_spec: Optional[str] = None,
    refresh: bool = False,
) -> None:
    """Get cluster status and resource availability."""
    # TODO: Implement cluster info
    raise NotImplementedError("get_cluster_info not yet implemented")
