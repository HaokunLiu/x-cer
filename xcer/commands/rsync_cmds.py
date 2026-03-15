from pathlib import Path
from ..utils import get_identity
from ..global_states import get_all_active_cluster
from ..multi_rsync import run_rsync_commands
from ..linked_dirs import find_path_via_closest_linked_dir


# TODO: Apply rsync_ignore_list from config after implementing config loading fucntion


def _build_rsync_flags(args):
    """Build rsync flags from parsed arguments."""
    flags = []

    if args.verbose:
        flags.append("-v")
    if args.archive:
        flags.append("-a")
    if args.recursive:
        flags.append("-r")
    if args.links:
        flags.append("-l")
    if args.compress:
        flags.append("-z")
    if hasattr(args, "preserve_perms") and args.preserve_perms:
        flags.append("-p")
    if args.times:
        flags.append("-t")
    if args.omit_dir_times:
        flags.append("-O")
    if hasattr(args, "update") and args.update:
        flags.append("-u")
    if args.dry_run:
        flags.append("--dry-run")
    if args.progress:
        flags.append("--progress")
    if args.partial:
        flags.append("--partial")
    if hasattr(args, "del") and getattr(args, "del"):
        flags.append("--del")
    if hasattr(args, "force") and args.force:
        flags.append("--force")
    if hasattr(args, "remove_source_files") and args.remove_source_files:
        flags.append("--remove-source-files")

    if hasattr(args, "exclude") and args.exclude:
        for pattern in args.exclude:
            flags.extend(["--exclude", pattern])

    if hasattr(args, "include") and args.include:
        for pattern in args.include:
            flags.extend(["--include", pattern])

    return " ".join(flags)


def xbroadcast_main(args):
    """Main function for xbroadcast command - broadcast files from source to destinations."""
    identity = get_identity()
    all_clusters = get_all_active_cluster()

    # Resolve source
    source = args.source if args.source else identity

    # Resolve destinations
    if args.destination:
        destinations = (
            args.destination
            if isinstance(args.destination, list)
            else [args.destination]
        )
    else:
        destinations = [c for c in all_clusters if c != source]

    # Convert path via linked directories
    original_path = Path(args.path)
    path = find_path_via_closest_linked_dir(original_path)
    flags = _build_rsync_flags(args)

    commands = {}

    for dest in destinations:
        if source == identity:
            # Local to remote
            cmd = f"rsync {flags} {path} {dest}:{path}"
        elif dest == identity:
            # Remote to local
            cmd = f"rsync {flags} {source}:{path} {path}"
        else:
            # Remote to remote via local (pull then push)
            cmd = f"rsync {flags} {source}:{path} {dest}:{path}"

        tag = f"{source} -> {dest}"
        commands[tag] = cmd

    if commands:
        print(
            f"Broadcasting {path} from {source} to {len(destinations)} destinations..."
        )
        results = run_rsync_commands(commands, report_results=True)

        success_count = sum(1 for code in results.values() if code == 0)
        total_count = len(results)
        print(f"Broadcast completed: {success_count}/{total_count} successful")
    else:
        print("No destinations to broadcast to.")


def xgather_main(args):
    """Main function for xgather command - gather files from sources to destination."""
    identity = get_identity()
    all_clusters = get_all_active_cluster()

    # Resolve destination
    destination = args.destination if args.destination else identity
    if isinstance(destination, list):
        if len(destination) > 1:
            raise ValueError("xgather can only have one destination")
        destination = destination[0]

    # Resolve sources
    if args.source:
        sources = args.source if isinstance(args.source, list) else [args.source]
    else:
        sources = [c for c in all_clusters if c != destination]

    # Convert path via linked directories
    original_path = Path(args.path)
    path = find_path_via_closest_linked_dir(original_path)
    flags = _build_rsync_flags(args)

    commands = {}

    for source in sources:
        if destination == identity:
            # Remote to local
            cmd = f"rsync {flags} {source}:{path} {path}"
        else:
            # Remote to remote via local (pull then push)
            cmd = f"rsync {flags} {source}:{path} {destination}:{path}"

        tag = f"{source} -> {destination}"
        commands[tag] = cmd

    if commands:
        print(f"Gathering {path} from {len(sources)} sources to {destination}...")
        results = run_rsync_commands(commands, report_results=True)

        success_count = sum(1 for code in results.values() if code == 0)
        total_count = len(results)
        print(f"Gather completed: {success_count}/{total_count} successful")
    else:
        print("No sources to gather from.")
