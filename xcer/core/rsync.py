"""Rsync operations for file sync."""

from typing import Optional


def broadcast_files(
    path: str,
    source: Optional[str],
    destinations: list[str],
    archive: bool = False,
    verbose: bool = False,
    compress: bool = False,
    update: bool = False,
    recursive: bool = False,
    links: bool = False,
    preserve_perms: bool = False,
    times: bool = False,
    omit_dir_times: bool = False,
    dry_run: bool = False,
    delete: bool = False,
    force: bool = False,
    excludes: Optional[list[str]] = None,
    includes: Optional[list[str]] = None,
    progress: bool = False,
    partial: bool = False,
) -> None:
    """Broadcast files from source to destination clusters."""
    # TODO: Implement using multi_rsync
    raise NotImplementedError("broadcast_files not yet implemented")


def gather_files(
    path: str,
    sources: list[str],
    destination: Optional[str],
    archive: bool = False,
    verbose: bool = False,
    compress: bool = False,
    recursive: bool = False,
    links: bool = False,
    preserve_perms: bool = False,
    times: bool = False,
    omit_dir_times: bool = False,
    dry_run: bool = False,
    excludes: Optional[list[str]] = None,
    includes: Optional[list[str]] = None,
    progress: bool = False,
    partial: bool = False,
    remove_source: bool = False,
) -> None:
    """Gather files from source clusters to destination."""
    # TODO: Implement using multi_rsync
    raise NotImplementedError("gather_files not yet implemented")
