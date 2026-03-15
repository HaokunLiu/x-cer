from dataclasses import dataclass
from typing import Literal
from enum import Enum, auto


@dataclass
class ClusterInfo:
    cluster_name: str
    hostname: str
    user: str
    group_name: str  # Might want to remove
    requires_tunnel: bool = False
    default_slurm_partition: str | None = None
    default_slurm_account: str | None = None
    default_slurm_qos: str | None = None
    filesystem_access: dict[str, str] | None = None


@dataclass
class PresetInfo:
    preset_name: str
    cluster_name: str
    description: str
    slurm_requests: str
    slurm_partition: str | None = None
    slurm_account: str | None = None
    slurm_qos: str | None = None


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
    resubmit_on_fail: bool = False
    max_resubmits: int = 0


@dataclass
class SystemConfig:
    heartbeat_interval: str
    refresh_interval: str
    show_ended_job: str
    job_rerun_cooldown: str
    rsync_ignore_list: list[str]
