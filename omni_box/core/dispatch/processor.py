from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.entities import InboxEvent
    from ..protocols.repository import InboxEventRepository
    from ..services.results import EventHandlerResult
    from .registry import EventRouter


def create_dispatching_handler(
    router: EventRouter,
    **dependencies: object,
) -> Callable[[InboxEvent, InboxEventRepository], Awaitable[EventHandlerResult]]:
    """Create handler function for EventBatchProcessor that uses router."""

    async def handler(event: InboxEvent, repo: InboxEventRepository) -> EventHandlerResult:
        return await router.dispatch(event, event.source, repo, **dependencies)

    return handler
