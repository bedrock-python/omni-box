"""Omni-box enums."""

from __future__ import annotations

from enum import StrEnum


class EventStatus(StrEnum):
    """General event status enumeration.

    Attributes:
        PENDING: Event is created and waiting to be processed/published.
        COMPLETED: Event has been successfully processed/published.
        FAILED: Event processing/publication failed after maximum retries.
    """

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
