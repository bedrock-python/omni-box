"""Outbox publisher protocols."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from .repository import EventRepository

if TYPE_CHECKING:
    from ..models.entities import OutboxEvent


class EventPublisher(ABC):
    """Abstract publisher that accepts OutboxEvent.

    The ``publish`` signature mirrors the handler signature used by
    :class:`~omni_box.core.pipeline.steps.handler.HandlerExecutionStep` so that
    publishers and inbox handlers are interchangeable from the pipeline's
    perspective. Most broker implementations do not need the repository; it is
    provided so a publisher *can* perform book-keeping in the same
    transaction (e.g. write an audit row) when desired.
    """

    @abstractmethod
    async def publish(self, event: OutboxEvent, repo: EventRepository[OutboxEvent]) -> None:
        """Publish event to the broker.

        Args:
            event: The outbox event to deliver.
            repo: Repository bound to the in-flight transaction. Pass-through
                value for the pipeline contract; broker implementations may
                ignore it.
        """
        ...
