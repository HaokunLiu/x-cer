This is a light weight tool to connect multiple compute clusters with disjoint storage and job management system. It is designed to make full use of Digital Research Alliance of Canada. 

Main goal:

    A researcher should be able to launch jobs, monitor jobs from any cluster or a laptop. One job can have multiple acceptable hardware configurations e.g. either L40S in cluster1 or A6000 in cluster2. Different job can have different environment requirement (conda envs, env vars).

    Design a set of bash and python utility scripts to help running experiments across multiple compute clusters.

    Cluster standardization: create links to standardize path. create customizable presets to standardize gpu types, partition, qos and account.

    Job management: squeue, sinfo, scancel, sbatch, but across cluster. (To use interactive session, log on to specific cluster is still necessary)

    File management: broadcast project code, cross cluster cp, gather experiment logs or artifacts. 

    Monitor cluster status: Track number of waiting jobs in the queue, top priority, personal and group storage quota.

    Setup guide and config guide: Update bashrc to help setting proxy etc.

Key concepts:

    linkdir: a set of path mappings on each cluster, it states things like linkdir scratch should be path_a in cluster1, and should be the same as path_b in cluster 2, so that the user can synchronize any child directory under a linkdir across clusters.

    preset: a set of computation requirement and environment setting on each cluster, that include the account, partition, qos for slurm, what type of GPUs to use, how many hours, what special preparation to use (module load, env activate, internet tunnel). The same preset from different cluster should be comparable. A job that can finish on cluster1’s presetX should also be able to finish on cluster2’s presetX.

User journey (one day):

    ssh to actively used clusters

    start background process (on servers that will receive jobs)

    run broadcast to update code

    submit jobs

    realized something is wrong, cancel bunch of jobs

    submit jobs again

    monitor jobs

    gather job outputs

User journey (long term):

    Get cluster access

    Modify cluster yaml

    Setup script create .xcer folder in this device and all the clusters, copy config to all the clusters, update ssh config

    Log onto each cluster, run xmonitor.

    Run xinfo to understand cluster resources available.

    Set up environment on each cluster.  

    To add or change settings, modify the config, run xcer apply, the config will be uploaded to the clusters. Upon daemon start, symlinks and sbatch scripts will be updated.

    To access the cluster from another personal device, copy .xcer and .ssh folder.

Background process (run on the login node of each cluster, but not on personal device)

    Modify flag file on successful init, and then checks the flag at each heartbeat, self-terminate if the flag belong to another process.

    Almost stateless (all its states are just for speedup, can be reconstructed from other sources)

    On heartbeat (30s), check my own jobs, launch assigned jobs, update job and cluster db

    On refresh (15min), check disk quota, check cluster sinfo, check other jobs in the system, update cluster and storage db.

    On detecting config change (checked during refresh), clean up cache states, rerun init.

Commands (see docstrings in xcer/cli/ for details):

    xcer broadcast: share a directory to all clusters by default

    xcer gather: collect files from all clusters (latest modification wins)

    xcer queue: check submitted jobs (just the user)

    xcer cancel: cancel selected jobs (by cluster:job_id or by job name)

    xcer info: check cluster status (available nodes)

    xcer submit: submit job to any cluster with free nodes

    xcer monitor: control monitor daemon on login nodes

    xcer notify job: notify user with email when job conditions met

    xcer notify quota: notify user when storage quota exceeds threshold

    xcer notify show/clear: manage notification requests

Local files

    ~/.xcer/linkdirs/ stores symlinks for linked directories

    ~/.xcer/config/ user-editable config files (clusters.yaml, presets.yaml, system.yaml)

    ~/.xcer/preset_sbatch/ auto generated sbatch scripts

    ~/.xcer/whereami.txt stores identity of the cluster

    ~/.xcer/mongodb_connection_str.txt stores MongoDB connection string

    ~/.xcer/monitor/ stores monitor daemon state (PID files, logs)

Information on MongoDB

    Job DB (indexed by issuer:job_id)

    Cluster status DB

    Storage status DB

    Notify request DB

