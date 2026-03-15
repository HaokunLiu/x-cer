# X-CER (Cross Cluster Experiment Remote)

Multi-cluster job management for HPC. Submit jobs, sync files, and monitor across clusters from anywhere.

## Install

```bash
pip install -e .
```

## Setup

### 1. SSH Config

Clusters need to SSH to each other for rsync. Add aliases to `~/.ssh/config`:

```
Host cluster1
    HostName login.cluster1.edu
    User myuser
    IdentityFile ~/.ssh/id_ed25519

Host cluster2
    HostName login.cluster2.edu
    User myuser
    IdentityFile ~/.ssh/id_ed25519
```

Ensure passwordless SSH works between all clusters:
```bash
# From cluster1, test SSH to cluster2
ssh cluster2 hostname

# If using Duo/MFA, you may need ControlMaster for connection reuse
```

### 2. MongoDB

X-CER uses MongoDB to track jobs and notifications across clusters. Options:

- **MongoDB Atlas** (recommended): Free tier works fine. Get connection string from Atlas dashboard.
- **Self-hosted**: Run MongoDB on a server accessible from all clusters.

Save connection string:
```bash
mkdir -p ~/.xcer
echo "mongodb+srv://user:pass@cluster.mongodb.net/xcer" > ~/.xcer/mongodb_connection_str.txt
```

### 3. Cluster Identity

Each machine needs to know its identity. On each cluster:
```bash
echo "cluster1" > ~/.xcer/whereami.txt  # Use actual cluster name
```

### 4. Linked Directories

Clusters mount storage at different paths. Create symlinks to standardize:

```bash
mkdir -p ~/.xcer/linkdirs

# Example: "scratch" means /scratch/user on cluster1, /home/user/scratch on cluster2
# On cluster1:
ln -s /scratch/myuser ~/.xcer/linkdirs/scratch

# On cluster2:
ln -s /home/myuser/scratch ~/.xcer/linkdirs/scratch
```

Now `~/.xcer/linkdirs/scratch/myproject` resolves correctly on both clusters.

### 5. Config Files

Create cluster and preset configs:

```bash
mkdir -p ~/.xcer/config
```

`~/.xcer/config/clusters.yaml`:
```yaml
clusters:
  cluster1:
    hostname: login.cluster1.edu
    user: myuser
    group_name: mygroup
    default_slurm_partition: gpu
    default_slurm_account: def-mypi
  cluster2:
    hostname: login.cluster2.edu
    user: myuser
    group_name: mygroup
    default_slurm_partition: compute
```

`~/.xcer/config/presets.yaml`:
```yaml
presets:
  gpu_l40s:
    description: "1x L40S GPU, 4 hours"
    clusters: [cluster1, cluster2]
    slurm_requests: "--gres=gpu:l40s:1 --time=4:00:00 --mem=32G"
  gpu_a100:
    description: "1x A100 GPU, 8 hours"
    clusters: [cluster1]
    slurm_requests: "--gres=gpu:a100:1 --time=8:00:00 --mem=64G"
```

`~/.xcer/config/system.yaml`:
```yaml
heartbeat_interval: "30s"
refresh_interval: "5m"
rsync_ignore_list:
  - ".git"
  - "__pycache__"
  - "*.pyc"
  - ".venv"
```

### 6. Start Monitor Daemon

On each cluster's login node (in tmux/screen for persistence):
```bash
xcer monitor start
```

### 7. Copy Setup to Other Machines

To use X-CER from another device (laptop, another cluster):
```bash
# Copy configs
scp -r ~/.xcer newmachine:~/

# Copy SSH config and keys
scp ~/.ssh/config newmachine:~/.ssh/
scp ~/.ssh/id_ed25519* newmachine:~/.ssh/
```

## Quick Start

### Check cluster resources
```bash
xcer info                        # Show all clusters and presets
xcer info -c cluster1 -p gpu_l40s  # Filter
xcer info --sort load            # Sort by load factor
```

### Submit jobs
```bash
xcer submit -p gpu_l40s my_job "python train.py"
xcer submit -p gpu_l40s -c cluster1,cluster2 my_job "python train.py"
xcer submit -p gpu_l40s -d data_prep my_job "python train.py"  # with dependency
```

### Monitor jobs
```bash
xcer queue --all      # All ongoing jobs
xcer queue "train*"   # Filter by name
xcer queue -r 1d      # Recent jobs (including finished)
```

### Cancel jobs
```bash
xcer cancel "train*"          # By name pattern
xcer cancel --all --dry-run   # Preview
```

### Sync files
```bash
xcer broadcast -avz /path/to/code                     # To all clusters
xcer broadcast -avz -d cluster1,cluster2 /path/to/code  # To specific clusters
xcer gather -avz /path/to/outputs                     # Collect from all
```

### Notifications
```bash
xcer notify job --all-done "train*"   # Email when jobs complete
xcer notify quota -p 90               # Email when quota > 90%
xcer notify show --all                # Show active notifications
```

## Documentation

- [DESIGN.md](docs/DESIGN.md) - Goals, key concepts (linkdir, preset), user journeys
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - Implementation structure, data flow, MongoDB schemas

## Requirements

- Python 3.9+
- MongoDB (Atlas free tier or self-hosted)
- SSH access between all clusters (passwordless or with ControlMaster)
- Shared filesystem between clusters (for monitor singleton coordination)

## Status

Work in progress. See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for implementation plan.
