"""Transaction provider protocols."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Protocol, runtime_checkable

from .repository import InboxEventRepository, OutboxEventRepository


@runtime_checkable
class InboxTransactionProviderProtocol(Protocol):
    """Provides transactional context for inbox repository."""

    def transaction(self) -> AbstractAsyncContextManager[InboxEventRepository]:
        """Open transaction and yield inbox repository."""
        ...


@runtime_checkable
class OutboxTransactionProviderProtocol(Protocol):
    """Provides transactional context for outbox repository."""

    def transaction(self) -> AbstractAsyncContextManager[OutboxEventRepository]:
        """Open transaction and yield outbox repository."""
        ...
