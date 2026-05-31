"""Metrics protocols (sync; Prometheus and other backends are sync)."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ProcessingMetrics(Protocol):
    """Common interface for metrics used during event processing."""

    def inc_processed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        """Increment count of successfully processed events."""
        ...

    def inc_failed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        """Increment count of failed processing attempts."""
        ...

    def inc_duplicate(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        """Increment count of duplicate/skipped events."""
        ...

    def observe_handler_duration(self, seconds: float, event_type: str | None = None) -> None:
        """Observe processing duration."""
        ...


@runtime_checkable
class OutboxMetrics(ProcessingMetrics, Protocol):
    """Interface for outbox processing metrics. All methods are sync."""

    def set_locked_batch_size(self, value: int) -> None:
        """Set the number of events locked in the current batch."""
        ...

    def inc_published(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        """Alias for inc_processed in outbox context."""
        ...


@runtime_checkable
class InboxMetrics(ProcessingMetrics, Protocol):
    """Interface for inbox consumer metrics. All methods are sync."""

    def inc_consumed(self, count: int = 1) -> None:
        """Increment the count of consumed broker messages."""
        ...

    def inc_committed(self, count: int = 1) -> None:
        """Increment the count of successful broker commits."""
        ...

    def inc_commit_failed(self, count: int = 1) -> None:
        """Increment the count of failed broker commits."""
        ...
