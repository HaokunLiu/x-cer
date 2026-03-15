"""Utilities for detecting and managing background sessions."""

import os
import subprocess
import sys


def is_in_background_session() -> tuple[bool, str]:
    """
    Check if running in a background-friendly session.
    
    Returns:
        Tuple of (is_background_safe, session_type)
        - is_background_safe: True if in nohup/screen/tmux/systemd
        - session_type: Description of detected session type
    """
    
    # Check for nohup
    if os.getenv('NOHUP'):
        return True, "nohup"
    
    # Check for screen
    if os.getenv('STY'):
        return True, f"screen (session: {os.getenv('STY')})"
    
    # Check for tmux
    if os.getenv('TMUX'):
        return True, f"tmux (session: {os.getenv('TMUX_PANE', 'unknown')})"
    
    # Check if parent process is init/systemd (daemon-like)
    try:
        ppid = os.getppid()
        if ppid == 1:
            return True, "systemd/init (parent PID 1)"
    except:
        pass
    
    # Check if running without controlling terminal
    try:
        # If we can't get terminal name, likely running detached
        tty = subprocess.check_output(['tty'], stderr=subprocess.DEVNULL).decode().strip()
        if 'not a tty' in tty:
            return True, "no controlling terminal"
    except (subprocess.CalledProcessError, FileNotFoundError):
        return True, "no controlling terminal"
    
    # Check if stdin/stdout are redirected (like with &)
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        return True, "redirected I/O"
    
    # If none of the above, likely running in regular shell
    return False, "interactive shell"


def warn_if_not_background():
    """Print warning if not running in a background-friendly session."""
    is_background, session_type = is_in_background_session()
    
    if not is_background:
        print("⚠️  WARNING: Monitor daemon is starting in an interactive shell session!")
        print("   This means the daemon will stop when you close your terminal/SSH session.")
        print("")
        print("   For persistent background execution, use one of:")
        print("   • nohup xmonitor start &")
        print("   • screen -S monitor xmonitor start")
        print("   • tmux new-session -d -s monitor xmonitor start")
        print("")
        print("   Continue anyway? (y/N): ", end="", flush=True)
        
        try:
            response = input().strip().lower()
            if response not in ['y', 'yes']:
                print("Aborted.")
                sys.exit(1)
        except (KeyboardInterrupt, EOFError):
            print("\nAborted.")
            sys.exit(1)
    else:
        print(f"✓ Running in background-safe session: {session_type}")


def start_detached(command_args: list[str]) -> int:
    """
    Start a process in detached mode.
    
    Args:
        command_args: List of command arguments to execute
        
    Returns:
        PID of the started process
    """
    import subprocess
    
    # Create new session and detach from parent
    process = subprocess.Popen(
        command_args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, 
        stdin=subprocess.DEVNULL,
        start_new_session=True,  # Detach from parent session
        cwd='/',  # Change to root directory
    )
    
    return process.pid