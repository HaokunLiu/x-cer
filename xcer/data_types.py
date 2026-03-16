from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto


@dataclass
class ClusterInfo:
    name: str
    hostname: str
    user: str
    group_name: str
    requires_tunnel: bool = False
    internal_login_node: str | None = None  # For two-hop SSH clusters
    note: str | None = None
    default_slurm_partition: str | None = None
    default_slurm_account: str | None = None
    default_slurm_qos: str | None = None


@dataclass
class PresetClusterConfig:
    """Cluster-specific overrides for a preset."""
    available: bool = True
    partition: str | None = None
    qos: str | None = None
    account: str | None = None


@dataclass
class PresetBase:
    """Base hardware configuration for a preset."""
    time: str | None = None
    mem: str | None = None
    cpus_per_task: int | None = None
    gres: str | None = None


@dataclass
class PresetInfo:
    """Hardware preset with base config and cluster-specific overrides."""
    name: str
    base: PresetBase
    cluster_configs: dict[str, PresetClusterConfig] = field(default_factory=dict)


@dataclass
class EnvironmentInfo:
    """Software environment preset."""
    name: str
    conda_env: str | None = None
    modules: list[str] = field(default_factory=list)
    env_vars: dict[str, str] = field(default_factory=dict)


class SlurmJobState(Enum):
    PENDING = "PD"
    RUNNING = "R"
    COMPLETING = "CG"
    COMPLETED = "CD"
    FAILED = "F"
    CANCELLED = "CA"
    TIMEOUT = "TO"
    TRANSMITTING = "TR"

    def is_terminal(self) -> bool:
        return self in {
            SlurmJobState.COMPLETED,
            SlurmJobState.FAILED,
            SlurmJobState.CANCELLED,
            SlurmJobState.TIMEOUT,
        }

    def is_successful(self) -> bool:
        return self == SlurmJobState.COMPLETED

    def is_unexpected(self) -> bool:
        return self in {
            SlurmJobState.FAILED,
            SlurmJobState.TIMEOUT,
        }

    def is_active(self) -> bool:
        return self in {
            SlurmJobState.TRANSMITTING,
            SlurmJobState.PENDING,
            SlurmJobState.RUNNING,
            SlurmJobState.COMPLETING,
        }


class NextAction(Enum):
    NONE = auto()
    MONITOR = auto()
    SUBMIT = auto()
    CANCEL = auto()
    RESUBMIT = auto()


# Note:
# on xrun, job created with SUBMIT.
# monitor check SUBMIT and RESUBMIT jobs, sends out commands, changes to MONITOR
# monitor check job status of all MONITOR jobs, if job moves to terminal state, depend on the resubmit request, change to NONE or RESUBMIT
# monitor check CANCEL jobs, sends out cancel command, change to NONE


@dataclass
class Job:
    job_name: str
    preset: str
    cluster_name: str  # The cluster that will receive the job
    issued_by: str  # Can be a cluster or personal device
    slurm_status: SlurmJobState
    next_action: NextAction
    slurm_id: str | None = None  # Assigned after sbatch
    submitted_at: datetime | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    exit_code: int | None = None
    resubmit_on_fail: bool = False
    max_resubmits: int = 0
    resubmit_count: int = 0
    dependency_job_name: str | None = None  # For job dependencies
    work_dir: str | None = None  # Working directory for the job
    command: str | None = None  # The command to run


@dataclass
class SystemConfig:
    heartbeat_interval: str
    refresh_interval: str
    show_ended_job: str
    job_rerun_cooldown: str
    rsync_ignore_list: list[str]
