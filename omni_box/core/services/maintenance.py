"""Omni-box maintenance service."""

from __future__ import annotations

import structlog

from ..constants import DEFAULT_CLEANUP_BATCH_SIZE, DEFAULT_MAINTENANCE_MAX_ITERATIONS
from ..exceptions import UnsupportedCapabilityError
from ..models.types import PositiveInt, PositiveNumber
from ..protocols.features import SupportsRetentionPolicies
from ..protocols.repository import EventRepository, RepositoryCapabilities

logger = structlog.get_logger(__name__)


class OmniBoxMaintenanceService:
    """Core service for omni-box maintenance operations."""

    def __init__(self, repo: EventRepository) -> None:
        self._repo = repo
        capabilities = getattr(repo, "capabilities", None)
        self._has_retention = (
            isinstance(capabilities, RepositoryCapabilities) and capabilities.supports_retention
        ) or isinstance(repo, SupportsRetentionPolicies)

    async def release_stale_locks(
        self,
        stale_timeout_seconds: PositiveNumber,
    ) -> int:
        if not self._has_retention:
            raise UnsupportedCapabilityError(
                capability="SupportsRetentionPolicies",
                repo_type=type(self._repo).__name__,
            )

        released_count = 0

        try:
            released_count = int(await self._repo.release_stale_locks(int(stale_timeout_seconds)))  # type: ignore[attr-defined]

            if released_count > 0:
                logger.info("Released stale locks", count=released_count, timeout_seconds=stale_timeout_seconds)
            else:
                logger.debug("No stale locks to release")
        except Exception:
            logger.exception("Failed to release stale locks", timeout_seconds=stale_timeout_seconds)
            raise
        return released_count

    async def cleanup_old_events(
        self,
        retention_days: PositiveInt,
        batch_size: int = DEFAULT_CLEANUP_BATCH_SIZE,
        max_iterations: int = DEFAULT_MAINTENANCE_MAX_ITERATIONS,
    ) -> int:
        if not self._has_retention:
            raise UnsupportedCapabilityError(
                capability="SupportsRetentionPolicies",
                repo_type=type(self._repo).__name__,
            )

        total_deleted = 0

        try:
            iteration = 0
            while iteration < max_iterations:
                deleted_count = int(
                    await self._repo.delete_old_completed(retention_days, batch_size=batch_size)  # type: ignore[attr-defined]
                )

                if deleted_count == 0:
                    break

                total_deleted += deleted_count
                iteration += 1

            if iteration >= max_iterations:
                logger.warning(
                    "Cleanup reached maximum iterations",
                    max_iterations=max_iterations,
                    total_deleted=total_deleted,
                )

            if total_deleted > 0:
                logger.info("Deleted old events", count=total_deleted, retention_days=retention_days)
            else:
                logger.debug("No old events to delete")
        except Exception:
            logger.exception("Failed to delete old events", retention_days=retention_days)
            raise
        return total_deleted
