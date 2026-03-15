"""Command line interface for monitor daemon testing."""

import sys
from xcer.monitor import Monitor
from xcer.data_types import SystemConfig


def main():
    """Simple test function for daemon functionality"""
    if len(sys.argv) < 2:
        print("Usage: python -m xcer.commands.monitor_cli [start|stop|status]")
        return

    action = sys.argv[1]
    monitor = Monitor()

    if action == "start":
        # Create a test config
        config = SystemConfig(
            heartbeat_interval="10s",
            refresh_interval="30s",
            show_ended_job="1h",
            job_rerun_cooldown="5m",
            rsync_ignore_list=[],
        )
        monitor.start_daemon(config)

    elif action == "stop":
        Monitor.stop_all_instances()
    elif action == "refresh":
        Monitor.request_refresh()
    else:
        print("Unknown action. Use start, stop or refresh.")


if __name__ == "__main__":
    main()
