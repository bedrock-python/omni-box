"""Prometheus metrics implementation."""

from __future__ import annotations

import re

from ...core.protocols.metrics import InboxMetrics, OutboxMetrics

_PREFIX_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _metric_name(name: str, prefix: str | None) -> str:
    if prefix:
        if not _PREFIX_PATTERN.match(prefix):
            raise ValueError(
                f"Invalid metric prefix: {prefix!r}. Must start with letter/underscore and contain only [a-zA-Z0-9_]."
            )
        return f"{prefix}_{name}"
    return name


try:
    from prometheus_client import Counter, Gauge, Histogram

    _HAS_PROMETHEUS = True
except ImportError:
    _HAS_PROMETHEUS = False
    Counter = None  # type: ignore[assignment,misc]
    Gauge = None  # type: ignore[assignment,misc]
    Histogram = None  # type: ignore[assignment,misc]


class PrometheusOutboxMetrics(OutboxMetrics):
    """Prometheus-backed implementation of OutboxMetrics.

    Requires prometheus_client (install with omni-box[metrics]).
    Metrics: outbox_locked_batch_size, outbox_events_published_total.
    """

    def __init__(self, prefix: str | None = None) -> None:
        if not _HAS_PROMETHEUS or Counter is None or Gauge is None:
            raise ImportError(
                "Outbox Prometheus metrics require prometheus-client. Install with: pip install omni-box[metrics]"
            )
        self._locked_batch_size = Gauge(
            _metric_name("outbox_locked_batch_size", prefix),
            "Number of locked events in the current processing batch",
        )
        self._events_published_total = Counter(
            _metric_name("outbox_events_published_total", prefix),
            "Total number of outbox events published",
            ["event_type", "status"],
        )
        self._handler_duration_seconds = Histogram(
            _metric_name("outbox_handler_duration_seconds", prefix),
            "Outbox handler (broker publish) execution duration in seconds",
            ["event_type"],
        )

    def set_locked_batch_size(self, value: int) -> None:
        self._locked_batch_size.set(value)

    def inc_published(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self._events_published_total.labels(event_type=event_type or "unknown", status=status or "success").inc(count)

    def inc_processed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self.inc_published(count, event_type=event_type, status=status)

    def inc_failed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self._events_published_total.labels(event_type=event_type or "unknown", status=status or "failure").inc(count)

    def inc_duplicate(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self._events_published_total.labels(event_type=event_type or "unknown", status=status or "skipped").inc(count)

    def observe_handler_duration(self, seconds: float, event_type: str | None = None) -> None:
        self._handler_duration_seconds.labels(event_type=event_type or "unknown").observe(seconds)


class PrometheusInboxMetrics(InboxMetrics):
    """Prometheus-backed implementation of InboxMetrics.

    Requires prometheus_client (install with omni-box[metrics]).
    """

    def __init__(self, prefix: str | None = None) -> None:
        if not _HAS_PROMETHEUS or Counter is None or Histogram is None:
            raise ImportError(
                "Inbox Prometheus metrics require prometheus-client. Install with: pip install omni-box[metrics]"
            )
        self._messages_consumed_total = Counter(
            _metric_name("inbox_messages_consumed_total", prefix),
            "Total number of consumed inbox messages",
        )
        self._messages_duplicate_total = Counter(
            _metric_name("inbox_messages_duplicate_total", prefix),
            "Total number of duplicate inbox messages",
        )
        self._messages_processed_total = Counter(
            _metric_name("inbox_messages_processed_total", prefix),
            "Total number of successfully processed inbox messages",
        )
        self._messages_failed_total = Counter(
            _metric_name("inbox_messages_failed_total", prefix),
            "Total number of failed inbox message processing attempts",
        )
        self._events_processed_total = Counter(
            _metric_name("inbox_events_processed_total", prefix),
            "Total number of inbox events processed",
            ["event_type", "status"],
        )
        self._messages_committed_total = Counter(
            _metric_name("inbox_messages_committed_total", prefix),
            "Total number of successful inbox broker commits",
        )
        self._commit_failures_total = Counter(
            _metric_name("inbox_commit_failures_total", prefix),
            "Total number of failed inbox broker commits",
        )
        self._handler_duration_seconds = Histogram(
            _metric_name("inbox_handler_duration_seconds", prefix),
            "Inbox handler execution duration in seconds",
            ["event_type"],
        )

    def inc_consumed(self, count: int = 1) -> None:
        self._messages_consumed_total.inc(count)

    def inc_duplicate(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self._messages_duplicate_total.inc(count)
        self._events_processed_total.labels(event_type=event_type or "unknown", status=status or "skipped").inc(count)

    def inc_processed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self._messages_processed_total.inc(count)
        self._events_processed_total.labels(event_type=event_type or "unknown", status=status or "success").inc(count)

    def inc_failed(self, count: int = 1, event_type: str | None = None, status: str | None = None) -> None:
        self._messages_failed_total.inc(count)
        self._events_processed_total.labels(event_type=event_type or "unknown", status=status or "failure").inc(count)

    def inc_committed(self, count: int = 1) -> None:
        self._messages_committed_total.inc(count)

    def inc_commit_failed(self, count: int = 1) -> None:
        self._commit_failures_total.inc(count)

    def observe_handler_duration(self, seconds: float, event_type: str | None = None) -> None:
        self._handler_duration_seconds.labels(event_type=event_type or "unknown").observe(seconds)
