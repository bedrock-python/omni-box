"""Unit tests for publisher protocols."""

from __future__ import annotations

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.protocols.publishers import EventPublisher
from omni_box.core.protocols.repository import EventRepository

pytestmark = pytest.mark.unit


def test__event_publisher__conforming_impl__passes_isinstance_check() -> None:
    # Arrange
    class MockPublisher(EventPublisher):
        async def publish(self, event: OutboxEvent, repo: EventRepository[OutboxEvent]) -> None:
            pass

    # Act / Assert
    assert isinstance(MockPublisher(), EventPublisher)
