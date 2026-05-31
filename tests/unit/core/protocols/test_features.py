"""Unit tests for ``omni_box.core.protocols.features``.

The module only defines runtime-checkable Protocols. These tests verify
their ``isinstance`` membership semantics for conforming and non-conforming
fakes.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from omni_box.core.models.entities import OutboxEvent
from omni_box.core.models.types import EventFailureUpdate
from omni_box.core.protocols.features import (
    SupportsBulkOperations,
    SupportsDistributedLocking,
    SupportsRetentionPolicies,
)

pytestmark = pytest.mark.unit


class _BulkRepo:
    async def bulk_mark_completed(self, event_ids: list[UUID], worker_id: str) -> int:
        return len(event_ids)

    async def bulk_create(self, events: list[OutboxEvent]) -> list[OutboxEvent]:
        return events

    async def bulk_mark_failed(
        self, failures: list[EventFailureUpdate], worker_id: str, count_as_attempt: bool = True
    ) -> int:
        return len(failures)

    async def bulk_release_locks(self, event_ids: list[UUID], worker_id: str) -> int:
        return len(event_ids)


class _LockingRepo:
    async def fetch_and_lock_pending(
        self, limit: int, worker_id: str, ttl: int | None = None, **filters: Any
    ) -> list[OutboxEvent]:
        return []

    async def refresh_lock(self, event_id: UUID, worker_id: str) -> bool:
        return True

    async def release_lock(self, event_id: UUID, worker_id: str) -> bool:
        return True

    async def force_unlock(self, event_id: UUID, reason: str) -> bool:
        return True


class _RetentionRepo:
    async def delete_old_completed(self, retention_days: int, batch_size: int) -> int:
        return 0

    async def release_stale_locks(self, stale_timeout_seconds: int) -> int:
        return 0


class _PartialBulkRepo:
    async def bulk_mark_completed(self, event_ids: list[UUID], worker_id: str) -> int:
        return 0

    # Missing bulk_create / bulk_mark_failed / bulk_release_locks


class _Empty:
    pass


def test__supports_bulk_operations__conforming_impl__passes_isinstance_check() -> None:
    assert isinstance(_BulkRepo(), SupportsBulkOperations)


def test__supports_bulk_operations__partial_impl__fails_isinstance_check() -> None:
    assert not isinstance(_PartialBulkRepo(), SupportsBulkOperations)


def test__supports_bulk_operations__unrelated__fails_isinstance_check() -> None:
    assert not isinstance(_Empty(), SupportsBulkOperations)


def test__supports_distributed_locking__conforming_impl__passes_isinstance_check() -> None:
    assert isinstance(_LockingRepo(), SupportsDistributedLocking)


def test__supports_distributed_locking__unrelated__fails_isinstance_check() -> None:
    assert not isinstance(_Empty(), SupportsDistributedLocking)


def test__supports_retention_policies__conforming_impl__passes_isinstance_check() -> None:
    assert isinstance(_RetentionRepo(), SupportsRetentionPolicies)


def test__supports_retention_policies__unrelated__fails_isinstance_check() -> None:
    assert not isinstance(_Empty(), SupportsRetentionPolicies)


@pytest.mark.parametrize(
    "instance,protocol,expected",
    [
        (_BulkRepo(), SupportsBulkOperations, True),
        (_LockingRepo(), SupportsDistributedLocking, True),
        (_RetentionRepo(), SupportsRetentionPolicies, True),
        (_Empty(), SupportsBulkOperations, False),
        (_Empty(), SupportsDistributedLocking, False),
        (_Empty(), SupportsRetentionPolicies, False),
    ],
    ids=[
        "bulk-ok",
        "locking-ok",
        "retention-ok",
        "empty-not-bulk",
        "empty-not-locking",
        "empty-not-retention",
    ],
)
def test__feature_protocols__various_pairs__isinstance_matches_expectation(
    instance: object, protocol: type, expected: bool
) -> None:
    # Arrange / Act / Assert
    assert isinstance(instance, protocol) is expected
