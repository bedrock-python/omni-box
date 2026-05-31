from collections.abc import Callable
from typing import Any

from .names import DispatchName, as_dispatch_str


def event_handler[F: Callable[..., Any]](
    event_type: DispatchName,
    topic: DispatchName | None = None,
    schema_version: str | None = None,
) -> Callable[[F], F]:
    """Mark method as event handler.

    Args:
        event_type: Event type to handle (``str`` or ``StrEnum``)
        topic: Optional topic override (``str`` or ``StrEnum``; class topic if omitted)
        schema_version: Optional schema version to match (e.g. "1.0.0")

    Returns:
        Decorated method with metadata attached
    """

    def decorator(method: F) -> F:
        method._is_event_handler = True  # type: ignore[attr-defined]
        method._event_type = as_dispatch_str(event_type)  # type: ignore[attr-defined]
        method._event_topic = as_dispatch_str(topic) if topic is not None else None  # type: ignore[attr-defined]
        method._schema_version = schema_version  # type: ignore[attr-defined]
        return method

    return decorator
