import pexpect
import time
from typing import Dict


def run_multi_rsync(
    commands: Dict[str, str], stale_timeout: int = 60
) -> Dict[str, int]:
    """
    Run multiple rsync commands in parallel with real-time tagged output.
    Automatically handles Duo Push and SSH fingerprint prompts.
    Reports stale processes when they haven't produced output for the specified timeout.

    Args:
        commands: Dict mapping tags to rsync commands
        stale_timeout: Seconds of inactivity before reporting stale status (default: 60)

    Returns:
        Dict mapping tags to return codes
    """
    import concurrent.futures

    def stream_command(tag: str, command: str) -> int:
        try:
            child = pexpect.spawn(command)
            last_output_time = time.time()
            timeout_count = 0

            while child.isalive():
                try:
                    index = child.expect(
                        [
                            r".*Passcode or option.*",
                            r".*Are you sure you want to continue connecting.*",
                            pexpect.EOF,
                            r".+",
                        ],
                        timeout=2,
                    )

                    def print_with_tag(child):
                        before = child.before.decode() if child.before else ""
                        after = child.after.decode() if child.after else ""
                        output = before + after
                        if not output.strip():
                            return

                        new_output = "\n".join(
                            f"[{tag}] {line}"
                            for line in output.split("\n")
                            if line.strip()
                        )
                        print(new_output)

                    if index == 0:  # Duo Push prompt
                        print_with_tag(child)
                        print(f"[{tag}] Auto-responding with '1' for Duo Push")
                        child.sendline("1")
                        last_output_time = time.time()
                        timeout_count = 0
                    elif index == 1:  # SSH fingerprint
                        print_with_tag(child)
                        print(f"[{tag}] Auto-responding with 'yes' for SSH fingerprint")
                        child.sendline("yes")
                        last_output_time = time.time()
                        timeout_count = 0
                    elif index == 2:  # EOF
                        break
                    elif index == 3:  # Regular output
                        print_with_tag(child)
                        last_output_time = time.time()
                        timeout_count = 0

                except pexpect.TIMEOUT:
                    timeout_count += 1
                    # Check if we've been stale for too long (timeout every 2s)
                    if timeout_count * 2 >= stale_timeout:
                        time_stale = time.time() - last_output_time
                        print(f"[{tag}] 🟡 STALE: No output for {time_stale:.0f}s")
                        timeout_count = 0  # Reset to avoid spam
                    continue
                except pexpect.EOF:
                    break

            child.close()
            exit_status = child.exitstatus or 0

            if exit_status == 0:
                print(f"[{tag}] 🟢 COMPLETED")
            else:
                print(f"[{tag}] 🔴 FAILED (exit code: {exit_status})")

            return exit_status

        except Exception as e:
            print(f"[{tag}] 🔴 FAILED: {e}")
            return 1

    # Run all commands in parallel using threads
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(stream_command, tag, cmd): tag
            for tag, cmd in commands.items()
        }

        results = {}
        for future in concurrent.futures.as_completed(futures):
            tag = futures[future]
            results[tag] = future.result()

    return results


def run_rsync_commands(
    commands: Dict[str, str], report_results: bool = False, stale_timeout: int = 60
) -> Dict[str, int]:
    """
    Run multiple rsync commands in parallel with real-time tagged output.
    Automatically handles Duo Push and SSH fingerprint prompts.
    Includes stale detection that reports status when processes become inactive.

    Args:
        commands: Dict mapping tags to rsync commands
        report_results: Whether to print final results summary
        stale_timeout: Seconds of inactivity before reporting stale status (default: 60)

    Returns:
        Dict mapping tags to return codes
    """
    results = run_multi_rsync(commands, stale_timeout)
    if report_results:
        for tag, returncode in results.items():
            status = "SUCCESS" if returncode == 0 else "FAILED"
            print(f"[{tag}] {status} (exit code: {returncode})")
    return results


# Example usage
if __name__ == "__main__":
    # Example rsync commands with tags
    import socket
    import os

    hostname = socket.gethostname()

    arg0 = os.getenv("MODE", "send")

    # dests = ["killarney"]
    dests = ["killarney", "vulcan", "trillium_gate"]
    # dests = ["killarney", "vulcan", "fir", "trillium"]
    # dests = ["killarney", "vulcan", "fir", "trillium_gate", "narval", "nibi", "rorqual"]

    # TEMPORARY
    if arg0 == "send":
        rsync_commands = {
            f"{hostname} -> {dest}": f"rsync -auvz --exclude='.git' --exclude='.venv' /Users/haokunl/Documents/GitHub/moose {dest}:~/s/code"
            for dest in dests
        }
    elif arg0 == "get":
        big_storage = "fir"
        rsync_commands = {
            f"{dest} -> {big_storage}": f"rsync -auvz {dest}:~/s/outputs {big_storage}:~/s/"
            for dest in dests
            if dest != big_storage
        }
    elif arg0 == "migrate":
        rsync_commands = {
            f"vi -> {dest}": f"rsync -auvz vi:~/s/outputs {dest}:~/s/" for dest in dests
        }
    elif arg0 == "cache":
        rsync_commands = {
            f"vulcan -> {dest}": f"rsync -auvz /scratch/haokun/.cache/huggingface {dest}:~/s/.cache"
            for dest in dests
            if dest != "vulcan"
        }

    print("Starting parallel rsync operations...")
    results = run_rsync_commands(rsync_commands, report_results=True)
    print("Rsync operations completed.")
