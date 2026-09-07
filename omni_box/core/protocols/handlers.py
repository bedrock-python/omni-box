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
        """Process an inbox event.

        ``repo`` is the repository the transaction provider yielded, bound to
        the transaction the event was inserted in.  With the PostgreSQL
        repository ``repo.session`` is that transaction's ``AsyncSession``:
        write the handler's side effects through it and they commit and roll
        back with the inbox row.  A session opened inside the handler is a
        second transaction and gives that up.  The parameter is typed as the
        protocol, which has no session, so a type-checked handler narrows it
        first: ``isinstance(repo, PostgresInboxRepository)``.
        """
        ...
