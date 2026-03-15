import re
from pathlib import Path

from xcer.paths import CLUSTER_IDENTITY_FILE


def expand_combined_flags(argv: list[str]) -> list[str]:
    """Convert combined short flags like -auv to -a -u -v"""
    expanded = []
    for arg in argv:
        if re.match(r"^-[a-zA-Z]{2,}$", arg):
            for char in arg[1:]:
                expanded.append(f"-{char}")
        else:
            expanded.append(arg)
    return expanded


def log_or_print(logger, message: str, level: str = "info"):
    """Log message if logger is available, otherwise print."""
    if logger:
        log_func = getattr(logger, level, logger.info)
        log_func(message)
    else:
        print(message)


def get_identity(allow_missing: bool = False) -> str:
    try:
        return CLUSTER_IDENTITY_FILE.read_text().strip()
    except FileNotFoundError:
        if allow_missing:
            # use hostname as identity
            import socket

            hostname = socket.gethostname()
            print(
                f"Warning: Identity file not found at {CLUSTER_IDENTITY_FILE}. Using hostname {hostname} as identity."
            )
            return hostname
        else:
            raise FileNotFoundError(
                f"Identity file not found at {CLUSTER_IDENTITY_FILE}. Please create this file with a unique identifier (which cluster is this?) for this device."
            )


def safe_touch(path: Path):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    except Exception as e:
        print(f"Failed to touch file {path}: {e}")


def safe_remove(path: Path):
    try:
        if path.exists():
            path.unlink()
    except Exception as e:
        print(f"Failed to delete file {path}: {e}")
