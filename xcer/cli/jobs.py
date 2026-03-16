"""
Job management commands: submit, queue, cancel.

submit - Submit a job to clusters with preset configuration
queue - Check submitted jobs across clusters
cancel - Cancel jobs across clusters
"""

from datetime import timedelta
from typing import Optional

from typing_extensions import Annotated
from pytimeparse import parse as parse_time

import typer

from xcer.mongo import get_mongodb_client
from xcer.services import submit as submit_service
from xcer.services import queue as queue_service
from xcer.services import cancel as cancel_service
from .common import Cluster, Preset, DryRun, parse_comma_list


def submit(
    name: Annotated[str, typer.Argument(help="Job name")],
    executable: Annotated[list[str], typer.Argument(help="Command to run")],
    preset: Annotated[str, typer.Option("-p", "--preset", help="Preset to use")] = ...,
    cluster: Cluster = None,
    dependency: Annotated[
        Optional[str], typer.Option("-d", "--dependency", help="Job names to wait for, comma-separated")
    ] = None,
    routing: Annotated[
        str,
        typer.Option(
            "-r", "--routing", help="Routing mode: round-robin, idle, load, throughput"
        ),
    ] = "load",
    dry_run: DryRun = False,
    retry: Annotated[int, typer.Option("--retry", help="Retry count on failure")] = 0,
    interactive: Annotated[bool, typer.Option("--interactive", help="Submit as interactive job")] = False,
) -> None:
    """
    Submit a job to clusters.

    \b
    Arguments:
        NAME        Job name
        EXECUTABLE  Command to run

    \b
    Examples:
        # Submit with a preset
        xcer submit -p gpu_l40s my_job "python train.py"

        # Submit to specific clusters
        xcer submit -p gpu_l40s -c cluster1,cluster2 my_job "python train.py"

        # Submit with dependency (wait for another job)
        xcer submit -p gpu_l40s -d prepare_data my_job "python train.py"

        # Interactive job (blocks and shows connection command)
        xcer submit -p gpu_interactive --interactive my_session bash

    \b
    Routing modes:
        round-robin  Cycle through clusters
        idle         Prefer clusters with most idle resources
        load         Prefer clusters with lowest load factor (default)
        throughput   Prefer clusters with highest throughput
    """
    client = get_mongodb_client()
    clusters_list = parse_comma_list(cluster)
    command = " ".join(executable)

    if dry_run:
        typer.echo(f"[DRY RUN] Would submit job '{name}' with preset '{preset}'")
        typer.echo(f"  Command: {command}")
        if clusters_list:
            typer.echo(f"  Clusters: {', '.join(clusters_list)}")
        typer.echo(f"  Routing: {routing}")
        if dependency:
            typer.echo(f"  Dependency: {dependency}")
        return

    if interactive:
        # TODO: Implement interactive mode - submit to all clusters, show allocation, let user choose
        typer.echo("Interactive mode not yet implemented", err=True)
        raise typer.Exit(1)

    try:
        job = submit_service.submit_job(
            client=client,
            job_name=name,
            preset_name=preset,
            command=command,
            cluster_names=clusters_list,
            dependency=dependency,
            resubmit_on_fail=retry > 0,
            max_resubmits=retry,
            strategy=routing,
        )
        typer.echo(f"Submitted job '{job.job_name}' to {job.cluster_name}")
        typer.echo(f"  Preset: {job.preset}")
        typer.echo(f"  Status: {job.next_action.name} (waiting for monitor)")
    except submit_service.SubmitError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


def queue(
    id_or_name: Annotated[Optional[str], typer.Argument(help="Job ID or name pattern")] = None,
    cluster: Cluster = None,
    name: Annotated[bool, typer.Option("-n", "--name", help="Treat argument as job name")] = False,
    id_: Annotated[bool, typer.Option("-i", "--id", help="Treat argument as job ID")] = False,
    state: Annotated[Optional[str], typer.Option("-t", "--state", help="Filter by state")] = None,
    preset: Preset = None,
    aggregate: Annotated[
        Optional[int], typer.Option("-a", "--aggregate", help="Aggregate jobs to MAX_COUNT groups")
    ] = None,
    recent: Annotated[
        Optional[str], typer.Option("-r", "--recent", help="Show jobs from last TIME (e.g. 1h, 2d)")
    ] = None,
    all_: Annotated[bool, typer.Option("--all", help="Show all ongoing jobs")] = False,
) -> None:
    """
    Check submitted jobs across clusters.

    \b
    Arguments:
        ID_OR_NAME  Job ID (host:123456) or name pattern (train*)
                    Pure numbers treated as ID, otherwise as name

    \b
    Examples:
        # Show all ongoing jobs
        xcer queue --all

        # Show jobs on specific cluster
        xcer queue -c cluster1

        # Show jobs matching pattern
        xcer queue train*

        # Show jobs by ID range
        xcer queue -c cluster1 host:123456..123460

        # Show recent jobs (including finished)
        xcer queue -r 1d

        # Filter by state
        xcer queue -t running
        xcer queue -t pending
    """
    client = get_mongodb_client()
    clusters_list = parse_comma_list(cluster)

    # Parse recent time
    include_recent = None
    if recent:
        seconds = parse_time(recent)
        if seconds:
            include_recent = timedelta(seconds=seconds)

    # Determine name pattern
    name_pattern = None
    if id_or_name and not id_:
        name_pattern = id_or_name

    jobs = queue_service.list_jobs(
        client=client,
        name_pattern=name_pattern,
        cluster_names=clusters_list,
        active_only=all_ and not recent,
        include_recent=include_recent,
    )

    if not jobs:
        typer.echo("No jobs found")
        return

    typer.echo(queue_service.format_job_table(jobs))


def cancel(
    id_or_name: Annotated[Optional[str], typer.Argument(help="Job ID or name pattern")] = None,
    cluster: Cluster = None,
    name: Annotated[bool, typer.Option("-n", "--name", help="Treat argument as job name")] = False,
    id_: Annotated[bool, typer.Option("-i", "--id", help="Treat argument as job ID")] = False,
    state: Annotated[Optional[str], typer.Option("-t", "--state", help="Filter by state")] = None,
    preset: Preset = None,
    aggregate: Annotated[
        Optional[int], typer.Option("-a", "--aggregate", help="Aggregate display to MAX_COUNT")
    ] = None,
    rollback: Annotated[
        Optional[str], typer.Option("-r", "--rollback", help="Cancel jobs from last TIME (e.g. 1h, 2d)")
    ] = None,
    delete: Annotated[bool, typer.Option("-d", "--delete", help="Delete jobs from database")] = False,
    all_: Annotated[bool, typer.Option("--all", help="Cancel all ongoing jobs")] = False,
    dry_run: DryRun = False,
) -> None:
    """
    Cancel jobs across clusters.

    \b
    Arguments:
        ID_OR_NAME  Job ID (host:123456) or name pattern (train*)
                    Pure numbers treated as ID, otherwise as name

    \b
    Examples:
        # Preview what would be cancelled
        xcer cancel --all --dry-run

        # Cancel by name pattern
        xcer cancel train*

        # Cancel specific job by ID
        xcer cancel -c cluster1 host:123456

        # Cancel jobs submitted in last hour
        xcer cancel -r 1h

        # Cancel and delete from database
        xcer cancel -d train*

        # Force treat as name (when name looks like ID)
        xcer cancel -n host:123456
    """
    client = get_mongodb_client()
    clusters_list = parse_comma_list(cluster)

    # Determine name pattern
    name_pattern = None
    if id_or_name and not id_:
        name_pattern = id_or_name
    elif all_:
        name_pattern = "*"

    if not name_pattern and not all_:
        typer.echo("Specify a job pattern or use --all", err=True)
        raise typer.Exit(1)

    try:
        jobs = cancel_service.cancel_jobs(
            client=client,
            name_pattern=name_pattern,
            cluster_names=clusters_list,
            dry_run=dry_run,
        )

        if dry_run:
            typer.echo(f"[DRY RUN] Would cancel {len(jobs)} job(s):")
        else:
            typer.echo(f"Cancelled {len(jobs)} job(s):")

        for job in jobs:
            typer.echo(f"  {job.job_name} on {job.cluster_name}")

    except cancel_service.CancelError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
