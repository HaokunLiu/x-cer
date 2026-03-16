"""Monitor package for background daemon functionality."""

from .daemon import MonitorBackbone
from . import heartbeat
from . import refresh
from . import alerts

# Alias for backward compatibility
Monitor = MonitorBackbone

__all__ = [
    "MonitorBackbone",
    "Monitor",
    "heartbeat",
    "refresh",
    "alerts",
]