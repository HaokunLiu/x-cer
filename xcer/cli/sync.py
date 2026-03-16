"""
File sync commands: broadcast, gather.

broadcast - Share directories and files across clusters
gather - Gather files from clusters (latest modification wins)
"""

from typing import Optional

from typing_extensions import Annotated

import typer

from xcer.services import sync as sync_service
from .common import DryRun, Verbose, parse_comma_list


def broadcast(
    path: Annotated[str, typer.Argument(help="Path to share")],
    source: Annotated[Optional[str], typer.Option("-s", "--source", help="Source cluster")] = None,
    destination: Annotated[
        Optional[str], typer.Option("-d", "--destination", help="Destination cluster(s), comma-separated")
    ] = None,
    archive: Annotated[bool, typer.Option("-a", "--archive", help="Archive mode (-rlpto)")] = False,
    verbose: Verbose = False,
    compress: Annotated[bool, typer.Option("-z", "--compress", help="Compress during transfer")] = False,
    update: Annotated[bool, typer.Option("-u", "--update", help="Skip newer files on receiver")] = False,
    recursive: Annotated[bool, typer.Option("-r", "--recursive", help="Recurse into directories")] = False,
    links: Annotated[bool, typer.Option("-l", "--links", help="Copy symlinks as symlinks")] = False,
    preserve_perms: Annotated[bool, typer.Option("--perms", help="Preserve permissions")] = False,
    times: Annotated[bool, typer.Option("-t", "--times", help="Preserve modification times")] = False,
    omit_dir_times: Annotated[bool, typer.Option("-O", "--omit-dir-times", help="Omit directory times")] = False,
    dry_run: DryRun = False,
    delete: Annotated[bool, typer.Option("--del", help="Delete extraneous files from destination")] = False,
    force: Annotated[bool, typer.Option("--force", help="Force deletion of non-empty directories")] = False,
    exclude: Annotated[Optional[str], typer.Option("--exclude", help="Exclude patterns, comma-separated")] = None,
    include: Annotated[Optional[str], typer.Option("--include", help="Include patterns, comma-separated")] = None,
    progress: Annotated[bool, typer.Option("--progress", help="Show progress during transfer")] = False,
    partial: Annotated[bool, typer.Option("--partial", help="Keep partially transferred files")] = False,
) -> None:
    """
    Share directories and files across clusters.

    By default, broadcasts from local cluster to all active clusters.

    \b
    Examples:
        # Send to specific clusters
        xcer broadcast -avz -d cluster1,cluster2 /path/to/share

        # Make destination exactly match source (delete extraneous files)
        xcer broadcast -avz --del --force -d cluster1 /path/to/share

        # Sync from one remote cluster to others
        xcer broadcast -avz -s cluster1 -d cluster2,cluster3 /path/to/share

    \b
    Common flags:
        -a  Archive mode (equivalent to -rlpt)
        -v  Verbose output
        -z  Compress during transfer
        -u  Skip files newer on receiver
    """
    # Build rsync flags
    flags = []
    if archive:
        flags.append("-a")
    if verbose:
        flags.append("-v")
    if compress:
        flags.append("-z")
    if update:
        flags.append("-u")
    if recursive and not archive:
        flags.append("-r")
    if links and not archive:
        flags.append("-l")
    if preserve_perms and not archive:
        flags.append("-p")
    if times and not archive:
        flags.append("-t")
    if omit_dir_times:
        flags.append("-O")
    if delete:
        flags.append("--delete")
    if force:
        flags.append("--force")
    if progress:
        flags.append("--progress")
    if partial:
        flags.append("--partial")

    rsync_flags = " ".join(flags) if flags else "-avz"
    excludes = parse_comma_list(exclude)
    destinations = parse_comma_list(destination)

    try:
        results = sync_service.broadcast(
            source_path=path,
            cluster_names=destinations,
            rsync_flags=rsync_flags,
            exclude=excludes,
            dry_run=dry_run,
        )

        # Report results
        success = sum(1 for code in results.values() if code == 0)
        failed = len(results) - success
        typer.echo(f"Broadcast complete: {success} succeeded, {failed} failed")

    except sync_service.SyncError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


def gather(
    path: Annotated[str, typer.Argument(help="Path to collect")],
    source: Annotated[
        Optional[str], typer.Option("-s", "--source", help="Source cluster(s), comma-separated")
    ] = None,
    destination: Annotated[
        Optional[str], typer.Option("-d", "--destination", help="Destination cluster")
    ] = None,
    archive: Annotated[bool, typer.Option("-a", "--archive", help="Archive mode (-rlpto)")] = False,
    verbose: Verbose = False,
    compress: Annotated[bool, typer.Option("-z", "--compress", help="Compress during transfer")] = False,
    recursive: Annotated[bool, typer.Option("-r", "--recursive", help="Recurse into directories")] = False,
    links: Annotated[bool, typer.Option("-l", "--links", help="Copy symlinks as symlinks")] = False,
    preserve_perms: Annotated[bool, typer.Option("--perms", help="Preserve permissions")] = False,
    times: Annotated[bool, typer.Option("-t", "--times", help="Preserve modification times")] = False,
    omit_dir_times: Annotated[bool, typer.Option("-O", "--omit-dir-times", help="Omit directory times")] = False,
    dry_run: DryRun = False,
    exclude: Annotated[Optional[str], typer.Option("--exclude", help="Exclude patterns, comma-separated")] = None,
    include: Annotated[Optional[str], typer.Option("--include", help="Include patterns, comma-separated")] = None,
    progress: Annotated[bool, typer.Option("--progress", help="Show progress during transfer")] = False,
    partial: Annotated[bool, typer.Option("--partial", help="Keep partially transferred files")] = False,
    remove_source: Annotated[
        bool, typer.Option("--remove-source-files", help="Remove source files after transfer")
    ] = False,
) -> None:
    """
    Gather files from clusters.

    If multiple sources have the same file, the latest modification wins.
    By default, gathers from all active clusters to local cluster.

    \b
    Examples:
        # Gather from specific clusters to local
        xcer gather -avz -s cluster1,cluster2 /path/to/collect

        # Gather from one cluster to another
        xcer gather -avz -s cluster1 -d cluster2 /path/to/collect

        # Remove source files after successful transfer
        xcer gather -avz --remove-source-files -s cluster1 /path/to/collect

    \b
    Common flags:
        -a  Archive mode (equivalent to -rlpt)
        -v  Verbose output
        -z  Compress during transfer
    """
    # Build rsync flags
    flags = []
    if archive:
        flags.append("-a")
    if verbose:
        flags.append("-v")
    if compress:
        flags.append("-z")
    if recursive and not archive:
        flags.append("-r")
    if links and not archive:
        flags.append("-l")
    if preserve_perms and not archive:
        flags.append("-p")
    if times and not archive:
        flags.append("-t")
    if omit_dir_times:
        flags.append("-O")
    if progress:
        flags.append("--progress")
    if partial:
        flags.append("--partial")
    if remove_source:
        flags.append("--remove-source-files")

    rsync_flags = " ".join(flags) if flags else "-avz"
    excludes = parse_comma_list(exclude)
    sources = parse_comma_list(source)

    try:
        results = sync_service.gather(
            source_path=path,
            cluster_names=sources,
            rsync_flags=rsync_flags,
            exclude=excludes,
            dry_run=dry_run,
        )

        # Report results
        success = sum(1 for code in results.values() if code == 0)
        failed = len(results) - success
        typer.echo(f"Gather complete: {success} succeeded, {failed} failed")

    except sync_service.SyncError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
