"""Common types for outbox contracts."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, NamedTuple
from uuid import UUID

from pydantic import BeforeValidator, Field


def _strip_and_check_empty(v: object) -> object:
    """Normalize string by stripping and ensuring it's not empty."""
    if isinstance(v, str):
        stripped = v.strip()
        if not stripped:
            raise ValueError("string cannot be empty or whitespace")
        return stripped
    return v


class EventFailureUpdate(NamedTuple):
    """Typed failure update item for bulk failure operations."""

    event_id: UUID
    error: str
    next_retry_at: datetime | None = None


# Type for positive integers (e.g. batch limits)
PositiveInt = Annotated[int, Field(gt=0)]

# Type for positive numbers (e.g. timeouts)
PositiveNumber = Annotated[float, Field(gt=0)]

# Type for strings that must be non-empty after stripping
StrippedNonEmptyStr = Annotated[
    str,
    BeforeValidator(_strip_and_check_empty),
]
