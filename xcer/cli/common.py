"""Shared CLI options and utilities."""

from typing import Optional

from typing_extensions import Annotated

import typer

# Shared type aliases for consistent options across commands
Cluster = Annotated[
    Optional[str],
    typer.Option("-c", "--cluster", help="Target cluster(s), comma-separated"),
]

Preset = Annotated[
    Optional[str],
    typer.Option("-p", "--preset", help="Preset name"),
]

DryRun = Annotated[
    bool,
    typer.Option("--dry-run", help="Show what would happen without executing"),
]

Verbose = Annotated[
    bool,
    typer.Option("-v", "--verbose", help="Increase verbosity"),
]


def parse_comma_list(value: Optional[str]) -> list[str]:
    """Parse comma-separated string into list."""
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]
