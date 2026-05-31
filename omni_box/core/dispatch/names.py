"""Topic / event-type strings for the event router (plain str or StrEnum)."""

from enum import StrEnum

DispatchName = str | StrEnum


def as_dispatch_str(name: DispatchName) -> str:
    """Normalize to str for registry keys; pass str or StrEnum (no ``.value``)."""
    return str(name)
