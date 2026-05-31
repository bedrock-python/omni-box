"""Metrics implementations: no-op metrics."""

from __future__ import annotations

from ..protocols.metrics import InboxMetrics, OutboxMetrics


class NoOpOutboxMetrics(OutboxMetrics):
    """No-op implementation of OutboxMetrics."""

    def set_locked_batch_size(self, value: int) -> None:
        pass

    def inc_published(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        pass

    def inc_processed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        pass

    def inc_failed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        pass

    def inc_duplicate(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        pass

    def observe_handler_duration(self, seconds: float, event_type: str | None = None) -> None:
        pass


class NoOpInboxMetrics(InboxMetrics):
    """No-op implementation of InboxMetrics."""

    def inc_consumed(self, count: int = 1) -> None:
        pass

    def inc_duplicate(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        pass

    def inc_processed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        pass

    def inc_failed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        pass

    def inc_committed(self, count: int = 1) -> None:
        pass

    def inc_commit_failed(self, count: int = 1) -> None:
        pass

    def observe_handler_duration(self, seconds: float, event_type: str | None = None) -> None:
        pass


__all__ = [
    "NoOpInboxMetrics",
    "NoOpOutboxMetrics",
]
