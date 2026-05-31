"""Unit tests for metrics protocols."""

from __future__ import annotations

import pytest

from omni_box.core.protocols.metrics import InboxMetrics, OutboxMetrics, ProcessingMetrics

pytestmark = pytest.mark.unit


def test__metrics_protocols__processing_metrics_conforming_impl__passes_isinstance_check() -> None:
    # Arrange
    class MockProcessingMetrics:
        def inc_processed(self, count=1):
            pass

        def inc_failed(self, count=1):
            pass

        def inc_duplicate(self, count=1):
            pass

        def observe_handler_duration(self, seconds):
            pass

    # Act / Assert
    assert isinstance(MockProcessingMetrics(), ProcessingMetrics)


def test__metrics_protocols__outbox_metrics_conforming_impl__passes_isinstance_check() -> None:
    # Arrange
    class MockOutboxMetrics:
        def inc_processed(self, count=1):
            pass

        def inc_failed(self, count=1):
            pass

        def inc_duplicate(self, count=1):
            pass

        def observe_handler_duration(self, seconds):
            pass

        def set_locked_batch_size(self, value):
            pass

        def inc_published(self, count=1):
            pass

    # Act / Assert
    assert isinstance(MockOutboxMetrics(), OutboxMetrics)


def test__metrics_protocols__inbox_metrics_conforming_impl__passes_isinstance_check() -> None:
    # Arrange
    class MockInboxMetrics:
        def inc_processed(self, count=1):
            pass

        def inc_failed(self, count=1):
            pass

        def inc_duplicate(self, count=1):
            pass

        def observe_handler_duration(self, seconds):
            pass

        def inc_consumed(self, count=1):
            pass

        def inc_committed(self, count=1):
            pass

        def inc_commit_failed(self, count=1):
            pass

    # Act / Assert
    assert isinstance(MockInboxMetrics(), InboxMetrics)
