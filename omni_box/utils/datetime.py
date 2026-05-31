"""Datetime utilities using stdlib only (no external deps)."""

from __future__ import annotations

from datetime import UTC, datetime


def utc_now() -> datetime:
    """Return current time as timezone-aware datetime in UTC."""
    return datetime.now(UTC)


def is_naive(dt: datetime) -> bool:
    """Check if a datetime object is naive (has no timezone info)."""
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None
