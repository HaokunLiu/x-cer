"""Notification management."""

from typing import Optional


def show_notifications(
    tag: Optional[str] = None,
    show_all: bool = False,
) -> None:
    """Show notification requests."""
    # TODO: Implement notification display
    raise NotImplementedError("show_notifications not yet implemented")


def clear_notifications(
    tag: Optional[str] = None,
    clear_all: bool = False,
) -> None:
    """Clear notification requests."""
    # TODO: Implement notification clearing
    raise NotImplementedError("clear_notifications not yet implemented")


def notify_on_job(
    id_or_name: list[str],
    by_name: bool = False,
    by_id: bool = False,
    preset: Optional[str] = None,
    clusters: Optional[list[str]] = None,
    recur: str = "1d",
    email: Optional[str] = None,
    tag: Optional[str] = None,
    all_done: bool = False,
    any_done: bool = False,
    num_done: Optional[int] = None,
    any_failed: bool = False,
    all_failed: bool = False,
    num_failed: Optional[int] = None,
) -> None:
    """Set up job notification."""
    # TODO: Implement job notifications
    raise NotImplementedError("notify_on_job not yet implemented")


def notify_on_quota(
    clusters: Optional[list[str]] = None,
    percent: int = 90,
    recur: str = "1d",
    email: Optional[str] = None,
    tag: Optional[str] = None,
) -> None:
    """Set up quota notification."""
    # TODO: Implement quota notifications
    raise NotImplementedError("notify_on_quota not yet implemented")
