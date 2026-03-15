from pathlib import Path


XCER_ROOT = Path("~/.xcer").expanduser()
CONFIG_FOLDER = XCER_ROOT / "config"
CONFIG_HASH_FILE = CONFIG_FOLDER / "config_hash.json"
LINKDIR_FOLDER = XCER_ROOT / "linkdirs"
PRESET_SBATCH_FOLDER = XCER_ROOT / "preset_sbatch"
CLUSTER_IDENTITY_FILE = XCER_ROOT / "whereami.txt"
MONGODB_CONNECTION_STR_FILE = XCER_ROOT / "mongodb_connection_str.txt"
MONITOR_ROOT = XCER_ROOT / "monitor"

if not XCER_ROOT.exists():
    XCER_ROOT.mkdir(parents=True, exist_ok=True)
