"""Unit tests for repository protocols."""

from __future__ import annotations

import pytest

from omni_box.core.protocols.repository import (
    InboxEventRepository,
    OutboxEventRepository,
)

pytestmark = pytest.mark.unit


def test__outbox_event_repository__conforming_impl__passes_isinstance_check() -> None:
    # Arrange
    class MockOutboxRepo:
        @property
        def capabilities(self):
            pass

        async def create(self, event):
            pass

        async def get_by_id(self, event_id):
            pass

        async def fetch_pending(self, limit, **filters):
            pass

        async def mark_processing(self, event_id, worker_id):
            pass

        async def mark_completed(self, event_id, worker_id):
            pass

        async def mark_failed(self, event_id, error, worker_id, next_retry_at, count_as_attempt=True):
            pass

    # Act / Assert
    assert isinstance(MockOutboxRepo(), OutboxEventRepository)


def test__inbox_event_repository__conforming_impl__passes_isinstance_check() -> None:
    # Arrange
    class MockInboxRepo:
        @property
        def capabilities(self):
            pass

        async def create(self, event):
            pass

        async def get_by_id(self, event_id):
            pass

        async def fetch_pending(self, limit, **filters):
            pass

        async def mark_processing(self, event_id, worker_id):
            pass

        async def mark_completed(self, event_id, worker_id):
            pass

        async def mark_failed(self, event_id, error, worker_id, next_retry_at, count_as_attempt=True):
            pass

        async def get_by_message_id(self, message_id, consumer_group):
            pass

        async def exists(self, message_id, consumer_group):
            pass

        async def has_completed_sibling_for_inbox_key(self, message_id, consumer_group, exclude_event_id):
            pass

    # Act / Assert
    assert isinstance(MockInboxRepo(), InboxEventRepository)
