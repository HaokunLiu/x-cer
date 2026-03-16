"""Config loading from YAML files."""

from pathlib import Path

import yaml

from xcer.data_types import (
    ClusterInfo,
    EnvironmentInfo,
    PresetBase,
    PresetClusterConfig,
    PresetInfo,
    SystemConfig,
)
from xcer.paths import CONFIG_FOLDER


class ConfigError(Exception):
    """Error loading or parsing configuration."""
    pass


def load_clusters(config_path: Path | None = None) -> list[ClusterInfo]:
    """Load cluster configurations from clusters.yaml.

    Args:
        config_path: Path to config folder. Defaults to ~/.xcer/config/

    Returns:
        List of ClusterInfo objects.
    """
    config_path = config_path or CONFIG_FOLDER
    clusters_file = config_path / "clusters.yaml"

    if not clusters_file.exists():
        raise ConfigError(f"Clusters config not found: {clusters_file}")

    with open(clusters_file) as f:
        data = yaml.safe_load(f)

    if not data or "clusters" not in data:
        raise ConfigError(f"Invalid clusters config: missing 'clusters' key")

    clusters = []
    for entry in data["clusters"]:
        cluster = ClusterInfo(
            name=entry["name"],
            hostname=entry["hostname"],
            user=entry["user"],
            group_name=entry["group_name"],
            requires_tunnel=entry.get("requires_tunnel", False),
            internal_login_node=entry.get("internal_login_node"),
            note=entry.get("note"),
            default_slurm_partition=entry.get("default_slurm_partition"),
            default_slurm_account=entry.get("default_slurm_account"),
            default_slurm_qos=entry.get("default_slurm_qos"),
        )
        clusters.append(cluster)

    return clusters


def load_presets(config_path: Path | None = None) -> list[PresetInfo]:
    """Load hardware presets from presets.yaml.

    Args:
        config_path: Path to config folder. Defaults to ~/.xcer/config/

    Returns:
        List of PresetInfo objects.
    """
    config_path = config_path or CONFIG_FOLDER
    presets_file = config_path / "presets.yaml"

    if not presets_file.exists():
        raise ConfigError(f"Presets config not found: {presets_file}")

    with open(presets_file) as f:
        data = yaml.safe_load(f)

    if not data or "presets" not in data:
        raise ConfigError(f"Invalid presets config: missing 'presets' key")

    presets = []
    for name, entry in data["presets"].items():
        base_data = entry.get("base", {})
        base = PresetBase(
            time=base_data.get("time"),
            mem=base_data.get("mem"),
            cpus_per_task=base_data.get("cpus-per-task"),
            gres=base_data.get("gres"),
        )

        cluster_configs = {}
        for cluster_name, cc_data in entry.get("cluster_configs", {}).items():
            cluster_configs[cluster_name] = PresetClusterConfig(
                available=cc_data.get("available", True),
                partition=cc_data.get("partition"),
                qos=cc_data.get("qos"),
                account=cc_data.get("account"),
            )

        preset = PresetInfo(
            name=name,
            base=base,
            cluster_configs=cluster_configs,
        )
        presets.append(preset)

    return presets


def load_environments(config_path: Path | None = None) -> list[EnvironmentInfo]:
    """Load environment presets from presets.yaml.

    Args:
        config_path: Path to config folder. Defaults to ~/.xcer/config/

    Returns:
        List of EnvironmentInfo objects.
    """
    config_path = config_path or CONFIG_FOLDER
    presets_file = config_path / "presets.yaml"

    if not presets_file.exists():
        raise ConfigError(f"Presets config not found: {presets_file}")

    with open(presets_file) as f:
        data = yaml.safe_load(f)

    environments = []
    for name, entry in data.get("environments", {}).items():
        env = EnvironmentInfo(
            name=name,
            conda_env=entry.get("conda_env"),
            modules=entry.get("modules", []),
            env_vars=entry.get("env_vars", {}),
        )
        environments.append(env)

    return environments


def load_system_config(config_path: Path | None = None) -> SystemConfig:
    """Load system configuration from system.yaml.

    Args:
        config_path: Path to config folder. Defaults to ~/.xcer/config/

    Returns:
        SystemConfig object.
    """
    config_path = config_path or CONFIG_FOLDER
    system_file = config_path / "system.yaml"

    if not system_file.exists():
        raise ConfigError(f"System config not found: {system_file}")

    with open(system_file) as f:
        data = yaml.safe_load(f)

    if not data or "system_config" not in data:
        raise ConfigError(f"Invalid system config: missing 'system_config' key")

    sc = data["system_config"]
    return SystemConfig(
        heartbeat_interval=sc["heartbeat_interval"],
        refresh_interval=sc["refresh_interval"],
        show_ended_job=sc["show_ended_job"],
        job_rerun_cooldown=sc["job_rerun_cooldown"],
        rsync_ignore_list=sc.get("rsync_ignore_list", []),
    )


def get_cluster(name: str, config_path: Path | None = None) -> ClusterInfo | None:
    """Get a specific cluster by name.

    Args:
        name: Cluster name to find.
        config_path: Path to config folder. Defaults to ~/.xcer/config/

    Returns:
        ClusterInfo if found, None otherwise.
    """
    clusters = load_clusters(config_path)
    for cluster in clusters:
        if cluster.name == name:
            return cluster
    return None


def get_preset(name: str, config_path: Path | None = None) -> PresetInfo | None:
    """Get a specific preset by name.

    Args:
        name: Preset name to find.
        config_path: Path to config folder. Defaults to ~/.xcer/config/

    Returns:
        PresetInfo if found, None otherwise.
    """
    presets = load_presets(config_path)
    for preset in presets:
        if preset.name == name:
            return preset
    return None


def get_preset_for_cluster(
    preset_name: str,
    cluster_name: str,
    config_path: Path | None = None,
) -> tuple[PresetInfo, PresetClusterConfig] | None:
    """Get a preset with its cluster-specific configuration.

    Args:
        preset_name: Name of the preset.
        cluster_name: Name of the cluster.
        config_path: Path to config folder. Defaults to ~/.xcer/config/

    Returns:
        Tuple of (PresetInfo, PresetClusterConfig) if available, None if preset
        doesn't exist or is not available on the specified cluster.
    """
    preset = get_preset(preset_name, config_path)
    if not preset:
        return None

    cluster_config = preset.cluster_configs.get(cluster_name, PresetClusterConfig())
    if not cluster_config.available:
        return None

    return (preset, cluster_config)


def build_slurm_args(preset: PresetInfo, cluster_config: PresetClusterConfig) -> str:
    """Build slurm sbatch arguments from preset and cluster config.

    Args:
        preset: The hardware preset.
        cluster_config: Cluster-specific overrides.

    Returns:
        String of sbatch arguments (e.g., "--time=24:00:00 --mem=32G --gres=gpu:1").
    """
    args = []
    base = preset.base

    if base.time:
        args.append(f"--time={base.time}")
    if base.mem:
        args.append(f"--mem={base.mem}")
    if base.cpus_per_task:
        args.append(f"--cpus-per-task={base.cpus_per_task}")
    if base.gres:
        args.append(f"--gres={base.gres}")

    if cluster_config.partition:
        args.append(f"--partition={cluster_config.partition}")
    if cluster_config.qos:
        args.append(f"--qos={cluster_config.qos}")
    if cluster_config.account:
        args.append(f"--account={cluster_config.account}")

    return " ".join(args)
