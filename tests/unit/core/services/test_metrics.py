"""Unit tests for ``omni_box.core.services.metrics`` (no-op implementations)."""

from __future__ import annotations

import pytest

from omni_box.core.protocols.metrics import InboxMetrics, OutboxMetrics
from omni_box.core.services.metrics import NoOpInboxMetrics, NoOpOutboxMetrics

pytestmark = pytest.mark.unit


# ---------- Protocol conformance ----------


def test__noop_outbox_metrics__instance__conforms_to_outbox_metrics_protocol() -> None:
    # Act / Assert
    assert isinstance(NoOpOutboxMetrics(), OutboxMetrics)


def test__noop_inbox_metrics__instance__conforms_to_inbox_metrics_protocol() -> None:
    # Act / Assert
    assert isinstance(NoOpInboxMetrics(), InboxMetrics)


# ---------- NoOpOutboxMetrics ----------


def test__noop_outbox_metrics__all_methods__return_none_without_error() -> None:
    # Arrange
    metrics = NoOpOutboxMetrics()

    # Act / Assert
    assert metrics.set_locked_batch_size(10) is None
    assert metrics.inc_published() is None
    assert metrics.inc_published(count=5, event_type="ev", status="ok") is None
    assert metrics.inc_processed() is None
    assert metrics.inc_processed(count=2, event_type="ev", status="ok") is None
    assert metrics.inc_failed() is None
    assert metrics.inc_failed(count=2, event_type="ev", status="err") is None
    assert metrics.inc_duplicate() is None
    assert metrics.inc_duplicate(count=2, event_type="ev", status="dup") is None
    assert metrics.observe_handler_duration(0.1) is None
    assert metrics.observe_handler_duration(0.2, event_type="ev") is None


# ---------- NoOpInboxMetrics ----------


def test__noop_inbox_metrics__all_methods__return_none_without_error() -> None:
    # Arrange
    metrics = NoOpInboxMetrics()

    # Act / Assert
    assert metrics.inc_consumed() is None
    assert metrics.inc_consumed(count=5) is None
    assert metrics.inc_duplicate() is None
    assert metrics.inc_duplicate(count=2, event_type="ev", status="dup") is None
    assert metrics.inc_processed() is None
    assert metrics.inc_processed(count=2, event_type="ev", status="ok") is None
    assert metrics.inc_failed() is None
    assert metrics.inc_failed(count=2, event_type="ev", status="err") is None
    assert metrics.inc_committed() is None
    assert metrics.inc_committed(count=3) is None
    assert metrics.inc_commit_failed() is None
    assert metrics.inc_commit_failed(count=2) is None
    assert metrics.observe_handler_duration(0.1) is None
    assert metrics.observe_handler_duration(0.2, event_type="ev") is None
