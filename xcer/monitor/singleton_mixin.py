"""Mixin for ensuring only one process runs across a cluster."""

from math import log
import socket
import os
import time
import tempfile
from pathlib import Path
from abc import ABC, abstractmethod
from glob import glob
from monitor.monitor_paths import MONITOR_PID_FOLDER

from xcer.utils import safe_touch, safe_remove


class SingletonMixin(ABC):
    """Mixin to ensure only one instance runs across the entire cluster."""

    def __init__(self, *args, **kwargs):
        self.my_signiture = f"{os.getpid()}:{socket.gethostname()}"
        MONITOR_PID_FOLDER.mkdir(parents=True, exist_ok=True)
        self._my_pid_file = MONITOR_PID_FOLDER / f"{self.my_signiture}.tmp"
        super().__init__(*args, **kwargs)

    @property
    @abstractmethod
    def heartbeat_interval(self) -> int:
        """Heartbeat interval in seconds. Must be implemented by the concrete class."""
        pass

    @property
    def _stale_threshold(self) -> int:
        """Threshold in seconds to consider a PID file stale."""
        return self.heartbeat_interval * 3

    @staticmethod
    def _find_pid_files() -> dict[Path, int]:
        """Find all PID files in the folder, return a dict of Path to age in seconds"""
        pid_files = glob(str(MONITOR_PID_FOLDER / "*.tmp"))
        current_time = time.time()
        path_with_age = {Path(f): current_time - os.path.getmtime(f) for f in pid_files}
        return path_with_age

    @staticmethod
    def _get_latest_pid_signiture() -> tuple[str, int] | None:
        """Get the latest (most recently modified) PID file signiture in the folder, return signiture and age in seconds"""
        path_with_age = list(SingletonMixin._find_pid_files().items())
        if not path_with_age:
            return None

        path_with_age.sort(key=lambda x: x[1])  # Sort by age (ascending)
        latest_path, age = path_with_age[0]
        signiture = latest_path.stem  # Filename without extension
        return signiture, age

    def _purge_stale_pid_files(self):
        """Remove stale PID files in the folder"""
        path_with_age = self._find_pid_files()
        for path, age in path_with_age.items():
            if age > self._stale_threshold:
                safe_remove(path)

    @staticmethod
    def _purge_all_pid_files():
        """Remove all PID files in the folder, static method."""
        path_with_age = SingletonMixin._find_pid_files()
        for path in path_with_age.keys():
            try:
                path.unlink()
                print(f"SingletonMixin: Removed PID file: {path}")
            except Exception as e:
                print(f"SingletonMixin: Failed to remove PID file {path}: {e}")
        print("SingletonMixin: All PID files removed.")

    def find_active_instance(self) -> str | None:
        """Check for existing instances. This should be called at before heavy initialization.

        Returns the signiture of the active instance if found, otherwise None.
        """
        latest = self._get_latest_pid_signiture()
        if not latest or latest[1] > self._stale_threshold:
            return None
        else:
            return latest[0]

    def maybe_start(self) -> bool:
        """Attempt to start this instance as the active instance, this should be called after heavy initialization.

        Returns True if this instance successfully started as the active instance.
        Returns False if another instance is already active.
        """
        self._purge_stale_pid_files()
        latest = self._get_latest_pid_signiture()
        if not latest or latest[1] > self._stale_threshold:
            safe_touch(self._my_pid_file)
            return True
        else:
            return False

    def maybe_continue(self) -> bool:
        """Check if this instance is still the active instance.

        Returns True if this instance is still active.
        Returns False if another instance has taken over.
        """
        latest = self._get_latest_pid_signiture()
        if latest and latest[0] == self.my_signiture:
            safe_touch(self._my_pid_file)
            return True
        else:
            return False

    def end_this_instance(self):
        """Clean up my PID file, effectively ending this instance"""
        safe_remove(self._my_pid_file)

    @staticmethod
    def end_all_instances():
        """Remove all PID files in the folder, effectively ending all instances"""
        SingletonMixin._purge_all_pid_files()

    def teardown(self):
        """Clean up my PID file on shutdown"""
        safe_remove(self._my_pid_file)
        super().teardown()
