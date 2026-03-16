"""Slurm command wrappers for remote execution."""

import re
from dataclasses import dataclass

from xcer.data_types import ClusterInfo, SlurmJobState
from xcer.remote.ssh import run_ssh_command, SSHError, SSHResult


class SlurmError(Exception):
    """Error executing Slurm command."""
    pass


@dataclass
class SbatchResult:
    """Result of sbatch submission."""
    slurm_id: str
    cluster_name: str


@dataclass
class SqueueJob:
    """Job info from squeue."""
    slurm_id: str
    name: str
    user: str
    state: SlurmJobState
    time_used: str
    nodes: str
    partition: str


@dataclass
class SinfoPartition:
    """Partition info from sinfo."""
    name: str
    state: str
    nodes_total: int
    nodes_idle: int
    nodes_allocated: int
    nodes_other: int


def sbatch(
    cluster: ClusterInfo,
    script_path: str,
    slurm_args: str = "",
    work_dir: str | None = None,
) -> SbatchResult:
    """Submit a job via sbatch.

    Args:
        cluster: Target cluster.
        script_path: Path to sbatch script on the cluster.
        slurm_args: Additional sbatch arguments.
        work_dir: Working directory for the job.

    Returns:
        SbatchResult with slurm job ID.

    Raises:
        SlurmError: If submission fails.
    """
    cmd_parts = ["sbatch"]
    if slurm_args:
        cmd_parts.append(slurm_args)
    if work_dir:
        cmd_parts.append(f"--chdir={work_dir}")
    cmd_parts.append(script_path)

    cmd = " ".join(cmd_parts)
    result = run_ssh_command(cluster, cmd)

    if result.exit_code != 0:
        raise SlurmError(f"sbatch failed on {cluster.name}: {result.stdout}")

    # Parse "Submitted batch job 12345"
    match = re.search(r"Submitted batch job (\d+)", result.stdout)
    if not match:
        raise SlurmError(f"Could not parse sbatch output: {result.stdout}")

    return SbatchResult(
        slurm_id=match.group(1),
        cluster_name=cluster.name,
    )


def sbatch_inline(
    cluster: ClusterInfo,
    command: str,
    job_name: str,
    slurm_args: str = "",
    work_dir: str | None = None,
    environment_setup: str = "",
) -> SbatchResult:
    """Submit a job with inline command (no script file needed).

    Args:
        cluster: Target cluster.
        command: Command to run.
        job_name: Name for the job.
        slurm_args: Slurm resource arguments.
        work_dir: Working directory.
        environment_setup: Commands to run before main command (module load, etc).

    Returns:
        SbatchResult with slurm job ID.
    """
    # Build inline sbatch script
    script_lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name={job_name}",
    ]

    if work_dir:
        script_lines.append(f"#SBATCH --chdir={work_dir}")

    # Add environment setup
    if environment_setup:
        script_lines.append("")
        script_lines.append("# Environment setup")
        script_lines.extend(environment_setup.split("\n"))

    script_lines.append("")
    script_lines.append("# Main command")
    script_lines.append(command)

    script_content = "\n".join(script_lines)

    # Use heredoc to submit inline
    sbatch_cmd = f"sbatch {slurm_args} << 'XCER_EOF'\n{script_content}\nXCER_EOF"

    result = run_ssh_command(cluster, sbatch_cmd)

    if result.exit_code != 0:
        raise SlurmError(f"sbatch failed on {cluster.name}: {result.stdout}")

    match = re.search(r"Submitted batch job (\d+)", result.stdout)
    if not match:
        raise SlurmError(f"Could not parse sbatch output: {result.stdout}")

    return SbatchResult(
        slurm_id=match.group(1),
        cluster_name=cluster.name,
    )


def squeue(
    cluster: ClusterInfo,
    user: str | None = None,
    job_ids: list[str] | None = None,
) -> list[SqueueJob]:
    """Get job queue status.

    Args:
        cluster: Target cluster.
        user: Filter by user (defaults to cluster user).
        job_ids: Filter by specific job IDs.

    Returns:
        List of SqueueJob objects.
    """
    user = user or cluster.user

    # Use machine-readable format
    cmd = f"squeue -u {user} -o '%i|%j|%u|%t|%M|%N|%P' --noheader"
    if job_ids:
        cmd = f"squeue -j {','.join(job_ids)} -o '%i|%j|%u|%t|%M|%N|%P' --noheader"

    result = run_ssh_command(cluster, cmd)

    if result.exit_code != 0:
        # Empty queue returns non-zero on some systems
        if "Invalid job id" in result.stdout or not result.stdout.strip():
            return []
        raise SlurmError(f"squeue failed on {cluster.name}: {result.stdout}")

    jobs = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue

        parts = line.strip().split("|")
        if len(parts) < 7:
            continue

        try:
            state = SlurmJobState(parts[3])
        except ValueError:
            # Unknown state, skip
            continue

        jobs.append(SqueueJob(
            slurm_id=parts[0],
            name=parts[1],
            user=parts[2],
            state=state,
            time_used=parts[4],
            nodes=parts[5],
            partition=parts[6],
        ))

    return jobs


def scancel(
    cluster: ClusterInfo,
    job_ids: list[str],
) -> bool:
    """Cancel jobs.

    Args:
        cluster: Target cluster.
        job_ids: List of job IDs to cancel.

    Returns:
        True if all cancellations succeeded.
    """
    if not job_ids:
        return True

    cmd = f"scancel {' '.join(job_ids)}"
    result = run_ssh_command(cluster, cmd)

    return result.exit_code == 0


def sinfo(
    cluster: ClusterInfo,
    partition: str | None = None,
) -> list[SinfoPartition]:
    """Get cluster partition info.

    Args:
        cluster: Target cluster.
        partition: Specific partition to query (None for all).

    Returns:
        List of SinfoPartition objects.
    """
    # Format: partition, state, total, idle, allocated, other
    cmd = "sinfo -o '%P|%a|%D|%i|%A' --noheader"
    if partition:
        cmd += f" -p {partition}"

    result = run_ssh_command(cluster, cmd)

    if result.exit_code != 0:
        raise SlurmError(f"sinfo failed on {cluster.name}: {result.stdout}")

    partitions = []
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue

        parts = line.strip().split("|")
        if len(parts) < 5:
            continue

        # Parse "allocated/idle" format in position 4
        alloc_idle = parts[4].split("/")
        allocated = int(alloc_idle[0]) if len(alloc_idle) > 0 else 0
        idle = int(alloc_idle[1]) if len(alloc_idle) > 1 else 0

        partitions.append(SinfoPartition(
            name=parts[0].rstrip("*"),  # Remove default partition marker
            state=parts[1],
            nodes_total=int(parts[2]) if parts[2].isdigit() else 0,
            nodes_idle=int(parts[3]) if parts[3].isdigit() else 0,
            nodes_allocated=allocated,
            nodes_other=0,
        ))

    return partitions


def sacct(
    cluster: ClusterInfo,
    job_id: str,
) -> dict:
    """Get job accounting info (exit code, runtime, etc).

    Args:
        cluster: Target cluster.
        job_id: Job ID to query.

    Returns:
        Dict with job accounting fields.
    """
    cmd = f"sacct -j {job_id} -o JobID,State,ExitCode,Start,End,Elapsed --noheader --parsable2"
    result = run_ssh_command(cluster, cmd)

    if result.exit_code != 0:
        raise SlurmError(f"sacct failed on {cluster.name}: {result.stdout}")

    # Parse first line (main job, not steps)
    for line in result.stdout.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.strip().split("|")
        if len(parts) >= 6 and not "." in parts[0]:  # Skip job steps
            exit_parts = parts[2].split(":")
            return {
                "job_id": parts[0],
                "state": parts[1],
                "exit_code": int(exit_parts[0]) if exit_parts[0].isdigit() else None,
                "signal": int(exit_parts[1]) if len(exit_parts) > 1 and exit_parts[1].isdigit() else None,
                "start_time": parts[3] if parts[3] != "Unknown" else None,
                "end_time": parts[4] if parts[4] != "Unknown" else None,
                "elapsed": parts[5],
            }

    return {}


def get_job_state(
    cluster: ClusterInfo,
    job_id: str,
) -> SlurmJobState | None:
    """Get current state of a specific job.

    Args:
        cluster: Target cluster.
        job_id: Job ID to check.

    Returns:
        SlurmJobState if job found, None otherwise.
    """
    # First try squeue (for active jobs)
    jobs = squeue(cluster, job_ids=[job_id])
    if jobs:
        return jobs[0].state

    # Try sacct for completed jobs
    acct = sacct(cluster, job_id)
    if acct:
        state_str = acct.get("state", "")
        # sacct uses different state codes
        state_map = {
            "COMPLETED": SlurmJobState.COMPLETED,
            "FAILED": SlurmJobState.FAILED,
            "CANCELLED": SlurmJobState.CANCELLED,
            "TIMEOUT": SlurmJobState.TIMEOUT,
            "PENDING": SlurmJobState.PENDING,
            "RUNNING": SlurmJobState.RUNNING,
        }
        return state_map.get(state_str)

    return None
