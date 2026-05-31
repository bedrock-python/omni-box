"""Unit tests for ``omni_box.core.services.maintenance``."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import pytest

from omni_box.core.exceptions import UnsupportedCapabilityError
from omni_box.core.models.entities import OutboxEvent
from omni_box.core.protocols.repository import RepositoryCapabilities
from omni_box.core.services.maintenance import OmniBoxMaintenanceService

pytestmark = pytest.mark.unit


class _BareRepo:
    """Repository that has no retention support — neither via capabilities nor protocol."""

    async def create(self, event: OutboxEvent) -> OutboxEvent:
        return event

    async def get_by_id(self, event_id: UUID) -> None:
        return None

    async def fetch_pending(self, limit: int, **filters: Any) -> list[OutboxEvent]:
        return []

    async def mark_processing(self, event_id: UUID, worker_id: str) -> bool:
        return True

    async def mark_completed(self, event_id: UUID, worker_id: str) -> None:
        return None

    async def mark_failed(
        self,
        event_id: UUID,
        error: str,
        worker_id: str,
        next_retry_at: datetime | None,
        count_as_attempt: bool = True,
    ) -> None:
        return None


class _RetentionRepo(_BareRepo):
    """Repository that structurally implements SupportsRetentionPolicies."""

    def __init__(
        self,
        *,
        delete_counts: list[int] | None = None,
        release_count: int = 0,
        delete_error: Exception | None = None,
        release_error: Exception | None = None,
    ) -> None:
        self._delete_counts = delete_counts or []
        self._release_count = release_count
        self._delete_error = delete_error
        self._release_error = release_error
        self.delete_calls: list[tuple[int, int]] = []
        self.release_calls: list[int] = []

    async def delete_old_completed(self, retention_days: int, batch_size: int) -> int:
        if self._delete_error:
            raise self._delete_error
        self.delete_calls.append((retention_days, batch_size))
        if not self._delete_counts:
            return 0
        return self._delete_counts.pop(0)

    async def release_stale_locks(self, stale_timeout_seconds: int) -> int:
        if self._release_error:
            raise self._release_error
        self.release_calls.append(stale_timeout_seconds)
        return self._release_count


class _CapabilitiesRetentionRepo(_RetentionRepo):
    """Retention support declared explicitly via capabilities flag."""

    @property
    def capabilities(self) -> RepositoryCapabilities:
        return RepositoryCapabilities(supports_retention=True)


async def test__release_stale_locks__bare_repo__raises_unsupported_capability() -> None:
    # Arrange
    service = OmniBoxMaintenanceService(_BareRepo())  # type: ignore[arg-type]

    # Act / Assert
    with pytest.raises(UnsupportedCapabilityError):
        await service.release_stale_locks(300)


async def test__release_stale_locks__retention_repo_returns_count__forwards_value() -> None:
    # Arrange
    repo = _RetentionRepo(release_count=5)
    service = OmniBoxMaintenanceService(repo)  # type: ignore[arg-type]

    # Act
    released = await service.release_stale_locks(300)

    # Assert
    assert released == 5
    assert repo.release_calls == [300]


async def test__release_stale_locks__zero_releases__returns_zero() -> None:
    # Arrange
    repo = _RetentionRepo(release_count=0)
    service = OmniBoxMaintenanceService(repo)  # type: ignore[arg-type]

    # Act
    released = await service.release_stale_locks(300)

    # Assert
    assert released == 0


async def test__release_stale_locks__repo_raises__exception_propagates() -> None:
    # Arrange
    repo = _RetentionRepo(release_error=RuntimeError("DB down"))
    service = OmniBoxMaintenanceService(repo)  # type: ignore[arg-type]

    # Act / Assert
    with pytest.raises(RuntimeError, match="DB down"):
        await service.release_stale_locks(300)


async def test__cleanup_old_events__bare_repo__raises_unsupported_capability() -> None:
    # Arrange
    service = OmniBoxMaintenanceService(_BareRepo())  # type: ignore[arg-type]

    # Act / Assert
    with pytest.raises(UnsupportedCapabilityError):
        await service.cleanup_old_events(7)


async def test__cleanup_old_events__deletes_until_empty__returns_total() -> None:
    # Arrange
    repo = _RetentionRepo(delete_counts=[10, 5, 0])
    service = OmniBoxMaintenanceService(repo)  # type: ignore[arg-type]

    # Act
    total = await service.cleanup_old_events(retention_days=7, batch_size=100)

    # Assert
    assert total == 15
    assert len(repo.delete_calls) == 3


async def test__cleanup_old_events__no_events_first_call__returns_zero() -> None:
    # Arrange
    repo = _RetentionRepo(delete_counts=[0])
    service = OmniBoxMaintenanceService(repo)  # type: ignore[arg-type]

    # Act
    total = await service.cleanup_old_events(7)

    # Assert
    assert total == 0


async def test__cleanup_old_events__max_iterations__caps_iterations() -> None:
    # Arrange: keep returning 10 forever -> only ``max_iterations`` calls allowed
    repo = _RetentionRepo(delete_counts=[10] * 100)
    service = OmniBoxMaintenanceService(repo)  # type: ignore[arg-type]
    max_iter = 3

    # Act
    total = await service.cleanup_old_events(retention_days=7, max_iterations=max_iter)

    # Assert
    assert total == 10 * max_iter
    assert len(repo.delete_calls) == max_iter


async def test__cleanup_old_events__repo_raises__exception_propagates() -> None:
    # Arrange
    repo = _RetentionRepo(delete_error=RuntimeError("boom"))
    service = OmniBoxMaintenanceService(repo)  # type: ignore[arg-type]

    # Act / Assert
    with pytest.raises(RuntimeError, match="boom"):
        await service.cleanup_old_events(7)


async def test__init__capabilities_object_with_retention_flag__enables_retention() -> None:
    # Arrange
    repo = _CapabilitiesRetentionRepo(release_count=2)
    service = OmniBoxMaintenanceService(repo)  # type: ignore[arg-type]

    # Act: should NOT raise UnsupportedCapabilityError
    released = await service.release_stale_locks(60)

    # Assert
    assert released == 2
