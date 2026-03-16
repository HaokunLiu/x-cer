"""SSH command execution wrapper using pexpect."""

import time
from dataclasses import dataclass

import pexpect

from xcer.data_types import ClusterInfo


@dataclass
class SSHResult:
    """Result of an SSH command execution."""
    stdout: str
    stderr: str
    exit_code: int
    timed_out: bool = False


class SSHError(Exception):
    """Error during SSH execution."""
    pass


def run_ssh_command(
    cluster: ClusterInfo,
    command: str,
    timeout: int = 60,
    handle_prompts: bool = True,
) -> SSHResult:
    """Run a command on a remote cluster via SSH.

    Args:
        cluster: Cluster configuration with hostname/user.
        command: Command to execute remotely.
        timeout: Max seconds to wait for command completion.
        handle_prompts: Auto-respond to Duo Push and SSH fingerprint prompts.

    Returns:
        SSHResult with stdout, stderr, and exit code.

    Raises:
        SSHError: If connection fails or command times out.
    """
    # Build SSH command
    if cluster.internal_login_node:
        # Two-hop SSH: first to gateway, then to internal node
        ssh_cmd = (
            f"ssh -o StrictHostKeyChecking=no {cluster.user}@{cluster.hostname} "
            f"'ssh {cluster.internal_login_node} \"{command}\"'"
        )
    else:
        ssh_cmd = (
            f"ssh -o StrictHostKeyChecking=no {cluster.user}@{cluster.hostname} "
            f"'{command}'"
        )

    try:
        child = pexpect.spawn(ssh_cmd, timeout=timeout, encoding="utf-8")
        output_lines = []

        while child.isalive():
            try:
                index = child.expect(
                    [
                        r".*Passcode or option.*",  # Duo Push
                        r".*Are you sure you want to continue connecting.*",  # SSH fingerprint
                        pexpect.EOF,
                        r".+",  # Regular output
                    ],
                    timeout=2,
                )

                if index == 0 and handle_prompts:  # Duo Push
                    child.sendline("1")
                elif index == 1 and handle_prompts:  # SSH fingerprint
                    child.sendline("yes")
                elif index == 2:  # EOF
                    break
                elif index == 3:  # Regular output
                    before = child.before or ""
                    after = child.after or ""
                    output = before + after
                    if output.strip():
                        output_lines.append(output)

            except pexpect.TIMEOUT:
                continue
            except pexpect.EOF:
                break

        child.close()
        exit_code = child.exitstatus or 0
        stdout = "".join(output_lines)

        return SSHResult(
            stdout=stdout,
            stderr="",
            exit_code=exit_code,
        )

    except pexpect.TIMEOUT:
        return SSHResult(
            stdout="",
            stderr=f"SSH command timed out after {timeout}s",
            exit_code=-1,
            timed_out=True,
        )
    except pexpect.ExceptionPexpect as e:
        raise SSHError(f"SSH connection failed: {e}")


def run_ssh_commands_parallel(
    commands: dict[ClusterInfo, str],
    timeout: int = 60,
) -> dict[str, SSHResult]:
    """Run SSH commands on multiple clusters in parallel.

    Args:
        commands: Dict mapping ClusterInfo to command string.
        timeout: Max seconds per command.

    Returns:
        Dict mapping cluster name to SSHResult.
    """
    import concurrent.futures

    def run_one(cluster: ClusterInfo, cmd: str) -> tuple[str, SSHResult]:
        result = run_ssh_command(cluster, cmd, timeout=timeout)
        return (cluster.name, result)

    results = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(run_one, cluster, cmd)
            for cluster, cmd in commands.items()
        ]

        for future in concurrent.futures.as_completed(futures):
            cluster_name, result = future.result()
            results[cluster_name] = result

    return results


def test_ssh_connection(cluster: ClusterInfo, timeout: int = 30) -> bool:
    """Test if SSH connection to cluster works.

    Args:
        cluster: Cluster to test.
        timeout: Connection timeout in seconds.

    Returns:
        True if connection successful, False otherwise.
    """
    try:
        result = run_ssh_command(cluster, "echo ok", timeout=timeout)
        return result.exit_code == 0 and "ok" in result.stdout
    except SSHError:
        return False
