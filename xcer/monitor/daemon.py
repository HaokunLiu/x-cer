"""Monitor daemon implementation for background process management."""

import signal
import time
import logging
import logging.handlers
import os
import sys
import subprocess
from pathlib import Path
from pytimeparse import parse as parse_time
from xcer.data_types import SystemConfig
from xcer.utils import safe_touch
from .monitor_paths import MONITOR_LOG_FOLDER, MONITOR_FORCE_REFRESH_FLAG
from .singleton_mixin import SingletonMixin
from .session_utils import warn_if_not_background, start_detached


class MonitorBackbone(SingletonMixin):
    def __init__(self):
        self.logger = None
        super().__init__()
        if signiture := self.find_active_instance():
            self.request_refresh()
            raise RuntimeError(
                f"Skipping starting monitor, another instance {signiture} is active. Run 'xmonitor stop' to stop it first."
            )
        self.config = None

    @staticmethod
    def request_refresh():
        """Create a temporary file to signal the monitor to refresh immediately."""
        safe_touch(MONITOR_FORCE_REFRESH_FLAG)

    @property
    def heartbeat_interval(self) -> int:
        """Heartbeat interval in seconds."""
        return parse_time(self._config.heartbeat_interval)

    def _setup_logging(self):
        """Setup file-based logging with rotation for daemon"""
        # Ensure log directory exists
        MONITOR_LOG_FOLDER.mkdir(parents=True, exist_ok=True)

        self._logger = logging.getLogger("Monitor")
        self._logger.setLevel(logging.INFO)

        # File handler with rotation
        log_file = MONITOR_LOG_FOLDER / "monitor.log"
        handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5  # 10MB, 5 backups
        )
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    def _init_from_config(self, config: SystemConfig):
        """Initialize monitor from system config, called at start or detection of config change"""
        self._config = config
        self._heartbeat_interval = parse_time(config.heartbeat_interval) or 30
        self._logger.info("Monitor initialized with config")

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""

        def signal_handler(signum, frame):
            self._logger.info(f"Received signal {signum}, shutting down gracefully")
            self.end_this_instance()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def _main_loop(self):
        """Main daemon loop handling both heartbeat and refresh"""
        self._logger.info("Starting main monitoring loop")
        last_conflict_check = 0
        conflict_check_interval = 30  # Check for conflicts every 30 seconds

        while self._running:
            try:
                current_time = time.time()

                # Check for cluster conflicts periodically
                if current_time - last_conflict_check >= conflict_check_interval:
                    if self._check_cluster_conflict():
                        self._logger.error("Cluster conflict detected, shutting down")
                        self._running = False
                        break
                    last_conflict_check = current_time

                # Check if heartbeat is due
                if current_time - self._last_heartbeat >= self._heartbeat_interval:
                    self.on_heartbeat()
                    self._last_heartbeat = current_time

                # Check if refresh is due
                refresh_interval = parse_time(self._config.refresh_interval) or 60
                if current_time - self._last_refresh >= refresh_interval:
                    self.on_refresh()
                    self._last_refresh = current_time

                # Sleep for a short interval to avoid busy waiting
                time.sleep(1)

            except Exception as e:
                self._logger.error(f"Error in main loop: {e}")
                time.sleep(5)  # Brief pause on error

        self._logger.info("Main monitoring loop stopped")

    def on_heartbeat(self):
        """Called periodically for heartbeat operations"""
        self._logger.info("Heartbeat")
        # Touch the PID file to indicate we're alive

    def on_refresh(self):
        """Called periodically for refresh operations"""
        self._logger.info("Refresh")
        # TODO: Implement refresh logic

    def start_daemon(self, config: SystemConfig, detached: bool = True):
        """Start the monitoring daemon as a background process"""
        # Check if already running
        if self.find_active_instance():
            print("Monitor daemon is already running")
            return

        if detached:
            # Check session and warn if needed
            warn_if_not_background()

            # Start detached process
            print("Starting monitor daemon in background...")
            pid = start_detached(
                [sys.executable, "-m", "xcer.commands.monitor_cli", "_run_daemon"]
            )
            print(f"Monitor daemon started with PID {pid}")
        else:
            # Run in foreground (for _run_daemon call)
            self._run_daemon_process(config)

    def _run_daemon_process(self, config: SystemConfig):
        """Internal method to run the actual daemon process"""
        try:
            # Change working directory and umask for daemon behavior
            os.chdir("/")
            os.umask(0o002)

            # Setup logging and config
            self._setup_logging()
            self._init_from_config(config)
            self._setup_signal_handlers()

            self._running = True
            self._last_heartbeat = time.time()
            self._last_refresh = time.time()

            self._logger.info("Monitor daemon process started")
            self._main_loop()

            self._logger.info("Monitor daemon process stopped")

        except Exception as e:
            if self._logger:
                self._logger.error(f"Failed to start daemon process: {e}")
            else:
                print(f"Failed to start daemon process: {e}")

        finally:
            # Clean up singleton on exit
            if hasattr(self, "teardown"):
                self.teardown()
