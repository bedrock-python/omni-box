"""Event handler protocols."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from ..models.entities import InboxEvent
    from ..services.results import EventHandlerResult
    from .repository import InboxEventRepository


class InboxHandler[T: InboxEvent](Protocol):
    """Unified protocol for inbox event handlers.

    ``**dependencies`` carries DI-resolved values injected by the runner
    (e.g. service instances, settings).  It is typed as ``Any`` because each
    handler implementation chooses its own keyword set; static checking is
    enforced at the handler signature, not on the protocol.
    """

    async def __call__(
        self,
        event: T,
        repo: InboxEventRepository,
        **dependencies: Any,
    ) -> EventHandlerResult | None:
        """Process an inbox event."""
        ...
