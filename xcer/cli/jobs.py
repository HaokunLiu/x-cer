"""
Job management commands: submit, queue, cancel.

submit - Submit a job to clusters with preset configuration
queue - Check submitted jobs across clusters
cancel - Cancel jobs across clusters
"""

from typing import Optional

from typing_extensions import Annotated

import typer

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
    from xcer.core.jobs import submit_job

    submit_job(
        name=name,
        executable=executable,
        preset=preset,
        clusters=parse_comma_list(cluster),
        dependencies=parse_comma_list(dependency),
        routing=routing,
        dry_run=dry_run,
        retry=retry,
        interactive=interactive,
    )


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
    from xcer.core.jobs import list_queue

    list_queue(
        id_or_name=id_or_name,
        clusters=parse_comma_list(cluster),
        by_name=name,
        by_id=id_,
        state=state,
        preset=preset,
        aggregate=aggregate,
        recent=recent,
        show_all=all_,
    )


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
    from xcer.core.jobs import cancel_jobs

    cancel_jobs(
        id_or_name=id_or_name,
        clusters=parse_comma_list(cluster),
        by_name=name,
        by_id=id_,
        state=state,
        preset=preset,
        aggregate=aggregate,
        rollback=rollback,
        delete_from_db=delete,
        cancel_all=all_,
        dry_run=dry_run,
    )
