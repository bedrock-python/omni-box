"""Unit tests for feature-based protocols."""

from __future__ import annotations

import pytest

from omni_box.core.protocols.features import (
    SupportsBulkOperations,
    SupportsDistributedLocking,
    SupportsRetentionPolicies,
)

pytestmark = pytest.mark.unit


def test__feature_protocols__bulk_ops_conforming_impl__passes_isinstance_check() -> None:
    # Arrange
    class MockBulkOps:
        async def bulk_mark_completed(self, event_ids, worker_id):
            pass

        async def bulk_create(self, events):
            pass

        async def bulk_mark_failed(self, failures, worker_id, count_as_attempt=True):
            pass

        async def bulk_release_locks(self, event_ids, worker_id):
            pass

    # Act / Assert
    assert isinstance(MockBulkOps(), SupportsBulkOperations)


def test__feature_protocols__distributed_locking_conforming_impl__passes_isinstance_check() -> None:
    # Arrange
    class MockLocking:
        async def fetch_and_lock_pending(self, limit, worker_id, ttl=None, **filters):
            pass

        async def refresh_lock(self, event_id, worker_id):
            pass

        async def release_lock(self, event_id, worker_id):
            pass

        async def force_unlock(self, event_id, reason):
            pass

    # Act / Assert
    assert isinstance(MockLocking(), SupportsDistributedLocking)


def test__feature_protocols__retention_conforming_impl__passes_isinstance_check() -> None:
    # Arrange
    class MockRetention:
        async def delete_old_completed(self, retention_days, batch_size):
            pass

        async def release_stale_locks(self, stale_timeout_seconds):
            pass

    # Act / Assert
    assert isinstance(MockRetention(), SupportsRetentionPolicies)
