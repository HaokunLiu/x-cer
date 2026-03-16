"""Notification request CRUD in MongoDB."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto

from pymongo import MongoClient
from pymongo.collection import Collection


DB_NAME = "xcer"
COLLECTION_NAME = "notifications"


class NotificationType(Enum):
    """Types of notification triggers."""
    JOB = auto()      # Job-related conditions
    QUOTA = auto()    # Quota thresholds
    CUSTOM = auto()   # Custom shell command


@dataclass
class JobCondition:
    """Conditions for job-based notifications."""
    job_patterns: list[str] = field(default_factory=list)  # Glob patterns
    clusters: list[str] = field(default_factory=list)      # Cluster filter
    all_done: bool = False      # Trigger when all matching jobs complete
    any_failed: bool = False    # Trigger on any job failure
    any_timeout: bool = False   # Trigger on any timeout


@dataclass
class QuotaCondition:
    """Conditions for quota-based notifications."""
    threshold_percent: float = 90.0  # Trigger when usage > threshold
    filesystem: str = "scratch"      # Which filesystem to check


@dataclass
class Notification:
    """A notification request."""
    tag: str                         # User-defined identifier
    notification_type: NotificationType
    email: str
    created_at: datetime
    recur_interval: timedelta | None = None  # None = one-shot
    last_triggered: datetime | None = None
    enabled: bool = True
    # Conditions (only one populated based on type)
    job_condition: JobCondition | None = None
    quota_condition: QuotaCondition | None = None
    custom_command: str | None = None  # For CUSTOM type


def _get_collection(client: MongoClient) -> Collection:
    """Get the notifications collection."""
    return client[DB_NAME][COLLECTION_NAME]


def _notification_to_doc(notif: Notification) -> dict:
    """Convert Notification to MongoDB document."""
    doc = {
        "_id": notif.tag,
        "tag": notif.tag,
        "notification_type": notif.notification_type.name,
        "email": notif.email,
        "created_at": notif.created_at,
        "recur_interval_seconds": notif.recur_interval.total_seconds() if notif.recur_interval else None,
        "last_triggered": notif.last_triggered,
        "enabled": notif.enabled,
    }

    if notif.job_condition:
        doc["job_condition"] = {
            "job_patterns": notif.job_condition.job_patterns,
            "clusters": notif.job_condition.clusters,
            "all_done": notif.job_condition.all_done,
            "any_failed": notif.job_condition.any_failed,
            "any_timeout": notif.job_condition.any_timeout,
        }

    if notif.quota_condition:
        doc["quota_condition"] = {
            "threshold_percent": notif.quota_condition.threshold_percent,
            "filesystem": notif.quota_condition.filesystem,
        }

    if notif.custom_command:
        doc["custom_command"] = notif.custom_command

    return doc


def _doc_to_notification(doc: dict) -> Notification:
    """Convert MongoDB document to Notification."""
    job_condition = None
    if "job_condition" in doc:
        jc = doc["job_condition"]
        job_condition = JobCondition(
            job_patterns=jc.get("job_patterns", []),
            clusters=jc.get("clusters", []),
            all_done=jc.get("all_done", False),
            any_failed=jc.get("any_failed", False),
            any_timeout=jc.get("any_timeout", False),
        )

    quota_condition = None
    if "quota_condition" in doc:
        qc = doc["quota_condition"]
        quota_condition = QuotaCondition(
            threshold_percent=qc.get("threshold_percent", 90.0),
            filesystem=qc.get("filesystem", "scratch"),
        )

    recur_secs = doc.get("recur_interval_seconds")
    recur_interval = timedelta(seconds=recur_secs) if recur_secs else None

    return Notification(
        tag=doc["tag"],
        notification_type=NotificationType[doc["notification_type"]],
        email=doc["email"],
        created_at=doc["created_at"],
        recur_interval=recur_interval,
        last_triggered=doc.get("last_triggered"),
        enabled=doc.get("enabled", True),
        job_condition=job_condition,
        quota_condition=quota_condition,
        custom_command=doc.get("custom_command"),
    )


def create_notification(client: MongoClient, notif: Notification) -> None:
    """Create or replace a notification.

    Args:
        client: MongoDB client.
        notif: Notification to create.
    """
    collection = _get_collection(client)
    doc = _notification_to_doc(notif)
    collection.replace_one({"_id": notif.tag}, doc, upsert=True)


def get_notification(client: MongoClient, tag: str) -> Notification | None:
    """Get a notification by tag.

    Args:
        client: MongoDB client.
        tag: Notification tag.

    Returns:
        Notification if found, None otherwise.
    """
    collection = _get_collection(client)
    doc = collection.find_one({"_id": tag})
    return _doc_to_notification(doc) if doc else None


def get_all_notifications(
    client: MongoClient,
    enabled_only: bool = True,
    notification_type: NotificationType | None = None,
) -> list[Notification]:
    """Get all notifications, optionally filtered.

    Args:
        client: MongoDB client.
        enabled_only: Only return enabled notifications.
        notification_type: Filter by type.

    Returns:
        List of Notification objects.
    """
    collection = _get_collection(client)
    query: dict = {}

    if enabled_only:
        query["enabled"] = True
    if notification_type:
        query["notification_type"] = notification_type.name

    cursor = collection.find(query)
    return [_doc_to_notification(doc) for doc in cursor]


def get_due_notifications(client: MongoClient) -> list[Notification]:
    """Get notifications that are due to be checked.

    Returns notifications that:
    - Are enabled
    - Have never triggered, OR
    - Are recurring and last_triggered + recur_interval <= now

    Args:
        client: MongoDB client.

    Returns:
        List of due Notification objects.
    """
    all_notifs = get_all_notifications(client, enabled_only=True)
    now = datetime.utcnow()
    due = []

    for notif in all_notifs:
        if notif.last_triggered is None:
            # Never triggered
            due.append(notif)
        elif notif.recur_interval:
            # Recurring: check if interval has passed
            next_time = notif.last_triggered + notif.recur_interval
            if now >= next_time:
                due.append(notif)
        # One-shot that already triggered: skip

    return due


def mark_triggered(client: MongoClient, tag: str) -> bool:
    """Mark a notification as triggered now.

    Args:
        client: MongoDB client.
        tag: Notification tag.

    Returns:
        True if notification was found and updated.
    """
    collection = _get_collection(client)
    result = collection.update_one(
        {"_id": tag},
        {"$set": {"last_triggered": datetime.utcnow()}},
    )
    return result.modified_count > 0


def disable_notification(client: MongoClient, tag: str) -> bool:
    """Disable a notification.

    Args:
        client: MongoDB client.
        tag: Notification tag.

    Returns:
        True if notification was found and disabled.
    """
    collection = _get_collection(client)
    result = collection.update_one(
        {"_id": tag},
        {"$set": {"enabled": False}},
    )
    return result.modified_count > 0


def enable_notification(client: MongoClient, tag: str) -> bool:
    """Enable a notification.

    Args:
        client: MongoDB client.
        tag: Notification tag.

    Returns:
        True if notification was found and enabled.
    """
    collection = _get_collection(client)
    result = collection.update_one(
        {"_id": tag},
        {"$set": {"enabled": True}},
    )
    return result.modified_count > 0


def delete_notification(client: MongoClient, tag: str) -> bool:
    """Delete a notification.

    Args:
        client: MongoDB client.
        tag: Notification tag.

    Returns:
        True if notification was found and deleted.
    """
    collection = _get_collection(client)
    result = collection.delete_one({"_id": tag})
    return result.deleted_count > 0


def create_job_notification(
    client: MongoClient,
    tag: str,
    email: str,
    job_patterns: list[str],
    clusters: list[str] | None = None,
    all_done: bool = False,
    any_failed: bool = False,
    any_timeout: bool = False,
    recur_hours: float | None = None,
) -> None:
    """Convenience function to create a job notification.

    Args:
        client: MongoDB client.
        tag: Unique tag for this notification.
        email: Email address to notify.
        job_patterns: Glob patterns for job names.
        clusters: Optional cluster filter.
        all_done: Trigger when all matching jobs complete.
        any_failed: Trigger on any failure.
        any_timeout: Trigger on any timeout.
        recur_hours: Hours between notifications (None for one-shot).
    """
    notif = Notification(
        tag=tag,
        notification_type=NotificationType.JOB,
        email=email,
        created_at=datetime.utcnow(),
        recur_interval=timedelta(hours=recur_hours) if recur_hours else None,
        job_condition=JobCondition(
            job_patterns=job_patterns,
            clusters=clusters or [],
            all_done=all_done,
            any_failed=any_failed,
            any_timeout=any_timeout,
        ),
    )
    create_notification(client, notif)


def create_quota_notification(
    client: MongoClient,
    tag: str,
    email: str,
    threshold_percent: float = 90.0,
    filesystem: str = "scratch",
    recur_hours: float = 24.0,
) -> None:
    """Convenience function to create a quota notification.

    Args:
        client: MongoDB client.
        tag: Unique tag for this notification.
        email: Email address to notify.
        threshold_percent: Trigger when usage exceeds this percent.
        filesystem: Which filesystem to monitor.
        recur_hours: Hours between notifications.
    """
    notif = Notification(
        tag=tag,
        notification_type=NotificationType.QUOTA,
        email=email,
        created_at=datetime.utcnow(),
        recur_interval=timedelta(hours=recur_hours),
        quota_condition=QuotaCondition(
            threshold_percent=threshold_percent,
            filesystem=filesystem,
        ),
    )
    create_notification(client, notif)
