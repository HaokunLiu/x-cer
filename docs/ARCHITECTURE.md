# X-CER Architecture Plan

## Overview

X-CER (Cross Cluster Experiment Remote) manages jobs and files across multiple HPC clusters. It provides:
- Job submission with intelligent routing
- Job monitoring and status tracking
- Cross-cluster file synchronization
- Email notifications for job/quota conditions
- Background daemon for continuous monitoring

## Current State

### Completed Modules

| Module | Purpose |
|--------|---------|
| `data_types.py` | Core types: `ClusterInfo`, `PresetInfo`, `Job`, `SlurmJobState`, `NextAction`, `SystemConfig` |
| `linked_dirs.py` | Cross-cluster path resolution via symlink mapping (clusters mount storage at different paths) |
| `multi_rsync.py` | Parallel rsync with pexpect, handles Duo Push and SSH prompts, stale detection |
| `paths.py` | Path constants (`~/.xcer/*`) |
| `utils.py` | Utilities: `expand_combined_flags`, `get_identity`, `safe_touch`, `safe_remove` |
| `cli/` | Typer-based CLI with all commands defined |
| `mongo/client.py` | MongoDB connection management |
| `monitor/daemon.py` | `MonitorBackbone` with heartbeat/refresh loop structure |
| `monitor/singleton_mixin.py` | Cluster-wide singleton via PID files on shared filesystem |
| `monitor/session_utils.py` | Background session detection, detached process startup |

### Modules to Build

| Module | Purpose |
|--------|---------|
| `config/loader.py` | Load `clusters.yaml` → `ClusterInfo`, `presets.yaml` → `PresetInfo` |
| `mongo/jobs.py` | Job CRUD: find by name/pattern, update state, delete |
| `mongo/notifications.py` | Notification request CRUD |
| `mongo/stats.py` | Cluster stats cache (for `xcer info`) |
| `remote/ssh.py` | SSH command execution wrapper (pexpect) |
| `remote/slurm.py` | Slurm commands: `sbatch`, `squeue`, `scancel`, `sinfo` |
| `services/submit.py` | Job submission orchestration |
| `services/queue.py` | Job listing orchestration |
| `services/cancel.py` | Job cancellation orchestration |
| `services/sync.py` | File sync orchestration (uses `multi_rsync.py`) |
| `services/info.py` | Cluster info orchestration |
| `services/notify.py` | Notification management |
| `monitor/heartbeat.py` | Poll squeue, update job states in DB |
| `monitor/refresh.py` | Poll sinfo, update stats cache |
| `monitor/alerts.py` | Evaluate notification conditions, send emails |

### To Delete

| Module | Reason |
|--------|--------|
| `core/` | Stub directory - functionality moves to `services/` |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                            CLI                                   │
│  cli/sync.py  cli/jobs.py  cli/basic.py  cli/notify.py         │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         SERVICES                                 │
│  Orchestration layer - coordinates mongo, remote, config        │
│                                                                  │
│  submit.py   queue.py   cancel.py   sync.py   info.py   notify.py│
└──────┬──────────────────────┬───────────────────────────────────┘
       │                      │
       ▼                      ▼
┌──────────────┐    ┌─────────────────┐    ┌──────────────────────┐
│    MONGO     │    │     REMOTE      │    │   EXISTING MODULES   │
│              │    │                 │    │                      │
│ jobs.py      │    │ ssh.py          │    │ multi_rsync.py       │
│ notifications│    │ slurm.py        │    │ linked_dirs.py       │
│ stats.py     │    │   sbatch        │    │ data_types.py        │
│ client.py ✓  │    │   squeue        │    │ config/loader.py     │
└──────────────┘    │   scancel       │    └──────────────────────┘
                    │   sinfo         │
                    └─────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                         MONITOR                                  │
│  Background daemon - keeps DB in sync with cluster reality      │
│                                                                  │
│  daemon.py ✓     singleton_mixin.py ✓     session_utils.py ✓   │
│  heartbeat.py    refresh.py              alerts.py              │
└─────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Job Submission (`xcer submit`)
```
CLI parses args
    │
    ▼
services/submit.py
    ├── config/loader.py: Load clusters, presets
    ├── data_types.py: routing logic (select cluster by load/idle/throughput)
    ├── remote/slurm.py: sbatch via SSH
    └── mongo/jobs.py: Save job record (name → slurm_id mapping)
```

### Job Query (`xcer queue`)
```
CLI parses args
    │
    ▼
services/queue.py
    ├── mongo/jobs.py: Find jobs by name pattern, state, time range
    ├── remote/slurm.py: Optionally poll squeue for live state
    └── Format and print output
```

### File Sync (`xcer broadcast`)
```
CLI parses args
    │
    ▼
services/sync.py
    ├── config/loader.py: Get cluster SSH info
    ├── linked_dirs.py: Resolve path via symlink mapping
    ├── Build rsync command strings
    └── multi_rsync.py: Execute in parallel with Duo handling
```

### Monitor Daemon
```
MonitorBackbone._main_loop()
    │
    ├── Every heartbeat_interval:
    │   └── heartbeat.py
    │       ├── remote/slurm.py: squeue on each cluster
    │       ├── mongo/jobs.py: Update job states
    │       └── Check Job.next_action state machine
    │
    ├── Every refresh_interval:
    │   └── refresh.py
    │       ├── remote/slurm.py: sinfo on each cluster
    │       └── mongo/stats.py: Update cached stats
    │
    └── On each cycle:
        └── alerts.py
            ├── mongo/notifications.py: Get active requests
            ├── mongo/jobs.py: Check job conditions
            └── Send emails if triggered
```

## Job State Machine

From `data_types.py`:

```
NextAction enum:
    SUBMIT    → Job created, waiting for monitor to submit
    MONITOR   → Job submitted, monitor tracking status
    RESUBMIT  → Job failed, will be resubmitted
    CANCEL    → Cancellation requested
    NONE      → Terminal state, no action needed

State transitions:
    User runs xcer submit
        → Job created with next_action=SUBMIT

    Monitor heartbeat sees SUBMIT
        → Runs sbatch
        → Sets next_action=MONITOR

    Monitor heartbeat sees MONITOR, job still running
        → No change

    Monitor heartbeat sees MONITOR, job completed
        → Sets next_action=NONE

    Monitor heartbeat sees MONITOR, job failed
        → If resubmit_on_fail: next_action=RESUBMIT
        → Else: next_action=NONE

    User runs xcer cancel
        → Sets next_action=CANCEL

    Monitor heartbeat sees CANCEL
        → Runs scancel
        → Sets next_action=NONE
```

## MongoDB Collections

### `jobs`
```json
{
    "_id": ObjectId,
    "job_name": "train_v1",
    "slurm_id": "123456",
    "cluster_name": "cluster1",
    "preset": "gpu_l40s",
    "issued_by": "macbook",
    "slurm_status": "RUNNING",
    "next_action": "MONITOR",
    "submitted_at": ISODate,
    "resubmit_on_fail": false,
    "max_resubmits": 0,
    "resubmit_count": 0
}
```

### `notifications`
```json
{
    "_id": ObjectId,
    "tag": "train_alert",
    "type": "job",
    "email": "user@example.com",
    "recur_seconds": 86400,
    "last_triggered": ISODate,
    "conditions": {
        "job_patterns": ["train*"],
        "clusters": ["cluster1"],
        "all_done": true,
        "any_failed": true
    }
}
```

### `cluster_stats`
```json
{
    "_id": ObjectId,
    "cluster_name": "cluster1",
    "preset": "gpu_l40s",
    "allocated": 32,
    "idle": 8,
    "requested": 4,
    "load_factor": 0.9,
    "throughput": 1250.5,
    "updated_at": ISODate
}
```

## Implementation Order

### Phase 1: Foundation
1. Delete `core/` stubs
2. Create `config/loader.py` - load cluster/preset YAMLs
3. Create `remote/ssh.py` - SSH command wrapper
4. Create `remote/slurm.py` - slurm command functions

### Phase 2: Database
5. Create `mongo/jobs.py` - Job CRUD
6. Create `mongo/stats.py` - Stats cache CRUD
7. Create `mongo/notifications.py` - Notification CRUD

### Phase 3: Services
8. Create `services/sync.py` - uses existing `multi_rsync.py` and `linked_dirs.py`
9. Create `services/submit.py`
10. Create `services/queue.py`
11. Create `services/cancel.py`
12. Create `services/info.py`
13. Create `services/notify.py`

### Phase 4: Monitor
14. Create `monitor/heartbeat.py`
15. Create `monitor/refresh.py`
16. Create `monitor/alerts.py`
17. Wire up in `monitor/daemon.py`

### Phase 5: Integration
18. Update CLI to call services instead of core stubs
19. End-to-end testing
20. Config template YAMLs

## Config Files

### `~/.xcer/config/clusters.yaml`
```yaml
clusters:
  cluster1:
    hostname: login.cluster1.edu
    user: myuser
    group_name: mygroup
    requires_tunnel: false
    default_slurm_partition: gpu
    default_slurm_account: myaccount
    filesystem_access:
      home: /home/myuser
      scratch: /scratch/myuser
```

### `~/.xcer/config/presets.yaml`
```yaml
presets:
  gpu_l40s:
    description: "1x L40S GPU, 4 hours"
    clusters: [cluster1, cluster2]
    slurm_requests: "--gres=gpu:l40s:1 --time=4:00:00 --mem=32G"
    slurm_partition: gpu

  gpu_a100:
    description: "1x A100 GPU, 8 hours"
    clusters: [cluster3]
    slurm_requests: "--gres=gpu:a100:1 --time=8:00:00 --mem=64G"
```

### `~/.xcer/config/system.yaml`
```yaml
heartbeat_interval: "30s"
refresh_interval: "5m"
show_ended_job: "1d"
job_rerun_cooldown: "10m"
rsync_ignore_list:
  - ".git"
  - "__pycache__"
  - "*.pyc"
  - ".venv"
```
