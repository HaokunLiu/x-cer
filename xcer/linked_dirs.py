from pathlib import Path

from xcer.paths import LINKDIR_FOLDER


def get_all_linked_directories() -> dict[Path, Path]:
    if not LINKDIR_FOLDER.exists():
        raise FileNotFoundError(
            f"Linked directories folder not found at {LINKDIR_FOLDER}"
        )
    linked_dir_mapping = {}

    for d in LINKDIR_FOLDER.iterdir():
        if d.is_symlink():
            linked_dir_mapping[d] = d.resolve()
        else:
            raise ValueError(
                f"Expected symlink for {d.name} (all files in {LINKDIR_FOLDER} must be symlinks)"
            )

    return linked_dir_mapping


def find_path_via_closest_linked_dir(path: Path) -> Path:
    real_target_path = path.resolve()
    linked_dirs = get_all_linked_directories()
    closest_dir = None
    closest_distance = float("inf")

    for linked_dir, real_linked_dir in linked_dirs.items():
        distance = len(real_target_path.relative_to(real_linked_dir).parts)
        if distance < closest_distance:
            closest_distance = distance
            closest_dir = linked_dir
            relative_path = real_target_path.relative_to(real_linked_dir)

    if closest_dir is None:
        raise ValueError(f"Path {path} is not within any linked directory")

    path_via_closest_linked_dir = closest_dir / relative_path
    return path_via_closest_linked_dir
