"""Unit tests for ``omni_box.infra.metrics.prometheus``."""

from __future__ import annotations

import importlib
import sys
from typing import TYPE_CHECKING

import pytest

import omni_box.infra.metrics.prometheus as prom_module
from omni_box.infra.metrics.prometheus import (
    PrometheusInboxMetrics,
    PrometheusOutboxMetrics,
    _metric_name,
)

if TYPE_CHECKING:
    pass

pytestmark = pytest.mark.unit


# Each test that registers Prometheus metrics needs a unique prefix because the
# default global registry forbids duplicate names within a process.
_PREFIX_COUNTER = {"n": 0}


def _unique_prefix(tag: str) -> str:
    _PREFIX_COUNTER["n"] += 1
    return f"ut_{tag}_{_PREFIX_COUNTER['n']}"


# -------- _metric_name --------


def test__metric_name__no_prefix__returns_name_as_is() -> None:
    # Act
    result = _metric_name("foo", None)

    # Assert
    assert result == "foo"


def test__metric_name__valid_prefix__prepends_with_underscore() -> None:
    # Act
    result = _metric_name("foo", "svc")

    # Assert
    assert result == "svc_foo"


def test__metric_name__valid_prefix_with_underscores__accepted() -> None:
    # Act
    result = _metric_name("foo", "_my_svc")

    # Assert
    assert result == "_my_svc_foo"


@pytest.mark.parametrize(
    "bad_prefix",
    ["123abc", "bar-baz", "spaces here", "dot.name", ""],
    ids=["starts-with-digit", "contains-hyphen", "contains-space", "contains-dot", "empty"],
)
def test__metric_name__invalid_prefix__raises_value_error(bad_prefix: str) -> None:
    if bad_prefix == "":
        # Empty falls into the "no prefix" branch, so it should NOT raise.
        assert _metric_name("foo", bad_prefix) == "foo"
        return

    # Act / Assert
    with pytest.raises(ValueError, match="Invalid metric prefix"):
        _metric_name("foo", bad_prefix)


# -------- PrometheusOutboxMetrics --------


def test__prometheus_outbox_metrics__missing_prometheus_client__raises_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setattr(prom_module, "_HAS_PROMETHEUS", False)
    monkeypatch.setattr(prom_module, "Counter", None)
    monkeypatch.setattr(prom_module, "Gauge", None)

    # Act / Assert
    with pytest.raises(ImportError, match="prometheus-client"):
        PrometheusOutboxMetrics(prefix=_unique_prefix("out_missing"))


def test__prometheus_outbox_metrics__set_locked_batch_size__updates_gauge() -> None:
    # Arrange
    metrics = PrometheusOutboxMetrics(prefix=_unique_prefix("out_lock"))

    # Act
    metrics.set_locked_batch_size(7)

    # Assert
    assert metrics._locked_batch_size._value.get() == 7  # type: ignore[attr-defined]


def test__prometheus_outbox_metrics__inc_published__updates_counter_with_labels() -> None:
    # Arrange
    metrics = PrometheusOutboxMetrics(prefix=_unique_prefix("out_pub"))

    # Act
    metrics.inc_published(2, event_type="t1", status="ok")

    # Assert
    sample = metrics._events_published_total.labels(event_type="t1", status="ok")
    assert sample._value.get() == 2  # type: ignore[attr-defined]


def test__prometheus_outbox_metrics__inc_published_no_labels__uses_defaults() -> None:
    # Arrange
    metrics = PrometheusOutboxMetrics(prefix=_unique_prefix("out_pub_def"))

    # Act
    metrics.inc_published()

    # Assert
    sample = metrics._events_published_total.labels(event_type="unknown", status="success")
    assert sample._value.get() == 1  # type: ignore[attr-defined]


def test__prometheus_outbox_metrics__inc_processed__delegates_to_inc_published() -> None:
    # Arrange
    metrics = PrometheusOutboxMetrics(prefix=_unique_prefix("out_proc"))

    # Act
    metrics.inc_processed(3, event_type="t2", status="success")

    # Assert
    sample = metrics._events_published_total.labels(event_type="t2", status="success")
    assert sample._value.get() == 3  # type: ignore[attr-defined]


def test__prometheus_outbox_metrics__inc_failed_no_labels__uses_failure_default() -> None:
    # Arrange
    metrics = PrometheusOutboxMetrics(prefix=_unique_prefix("out_fail"))

    # Act
    metrics.inc_failed()

    # Assert
    sample = metrics._events_published_total.labels(event_type="unknown", status="failure")
    assert sample._value.get() == 1  # type: ignore[attr-defined]


def test__prometheus_outbox_metrics__inc_duplicate__uses_skipped_status_default() -> None:
    # Arrange
    metrics = PrometheusOutboxMetrics(prefix=_unique_prefix("out_dup"))

    # Act
    metrics.inc_duplicate()

    # Assert
    sample = metrics._events_published_total.labels(event_type="unknown", status="skipped")
    assert sample._value.get() == 1  # type: ignore[attr-defined]


def test__prometheus_outbox_metrics__observe_handler_duration__updates_histogram() -> None:
    # Arrange
    metrics = PrometheusOutboxMetrics(prefix=_unique_prefix("out_dur"))

    # Act
    metrics.observe_handler_duration(0.12, event_type="t3")

    # Assert
    hist = metrics._handler_duration_seconds.labels(event_type="t3")
    assert hist._sum.get() == pytest.approx(0.12)  # type: ignore[attr-defined]


def test__prometheus_outbox_metrics__observe_handler_duration_no_label__uses_unknown() -> None:
    # Arrange
    metrics = PrometheusOutboxMetrics(prefix=_unique_prefix("out_dur_def"))

    # Act
    metrics.observe_handler_duration(0.5)

    # Assert
    hist = metrics._handler_duration_seconds.labels(event_type="unknown")
    assert hist._sum.get() == pytest.approx(0.5)  # type: ignore[attr-defined]


# -------- PrometheusInboxMetrics --------


def test__prometheus_inbox_metrics__missing_prometheus_client__raises_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setattr(prom_module, "_HAS_PROMETHEUS", False)
    monkeypatch.setattr(prom_module, "Counter", None)
    monkeypatch.setattr(prom_module, "Histogram", None)

    # Act / Assert
    with pytest.raises(ImportError, match="prometheus-client"):
        PrometheusInboxMetrics(prefix=_unique_prefix("in_missing"))


def test__prometheus_inbox_metrics__inc_consumed__increments_counter() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_cons"))

    # Act
    metrics.inc_consumed(4)

    # Assert
    assert metrics._messages_consumed_total._value.get() == 4  # type: ignore[attr-defined]


def test__prometheus_inbox_metrics__inc_duplicate__updates_two_counters() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_dup"))

    # Act
    metrics.inc_duplicate(2, event_type="ev", status="skipped")

    # Assert
    assert metrics._messages_duplicate_total._value.get() == 2  # type: ignore[attr-defined]
    sample = metrics._events_processed_total.labels(event_type="ev", status="skipped")
    assert sample._value.get() == 2  # type: ignore[attr-defined]


def test__prometheus_inbox_metrics__inc_duplicate_no_labels__uses_defaults() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_dup_def"))

    # Act
    metrics.inc_duplicate()

    # Assert
    sample = metrics._events_processed_total.labels(event_type="unknown", status="skipped")
    assert sample._value.get() == 1  # type: ignore[attr-defined]


def test__prometheus_inbox_metrics__inc_processed__updates_two_counters() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_proc"))

    # Act
    metrics.inc_processed(3, event_type="ev", status="success")

    # Assert
    assert metrics._messages_processed_total._value.get() == 3  # type: ignore[attr-defined]
    sample = metrics._events_processed_total.labels(event_type="ev", status="success")
    assert sample._value.get() == 3  # type: ignore[attr-defined]


def test__prometheus_inbox_metrics__inc_processed_no_labels__uses_defaults() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_proc_def"))

    # Act
    metrics.inc_processed()

    # Assert
    sample = metrics._events_processed_total.labels(event_type="unknown", status="success")
    assert sample._value.get() == 1  # type: ignore[attr-defined]


def test__prometheus_inbox_metrics__inc_failed__updates_two_counters() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_fail"))

    # Act
    metrics.inc_failed(1, event_type="ev", status="failure")

    # Assert
    assert metrics._messages_failed_total._value.get() == 1  # type: ignore[attr-defined]
    sample = metrics._events_processed_total.labels(event_type="ev", status="failure")
    assert sample._value.get() == 1  # type: ignore[attr-defined]


def test__prometheus_inbox_metrics__inc_failed_no_labels__uses_defaults() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_fail_def"))

    # Act
    metrics.inc_failed()

    # Assert
    sample = metrics._events_processed_total.labels(event_type="unknown", status="failure")
    assert sample._value.get() == 1  # type: ignore[attr-defined]


def test__prometheus_inbox_metrics__inc_committed__increments_counter() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_commit"))

    # Act
    metrics.inc_committed(5)

    # Assert
    assert metrics._messages_committed_total._value.get() == 5  # type: ignore[attr-defined]


def test__prometheus_inbox_metrics__inc_commit_failed__increments_counter() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_cfail"))

    # Act
    metrics.inc_commit_failed(2)

    # Assert
    assert metrics._commit_failures_total._value.get() == 2  # type: ignore[attr-defined]


def test__prometheus_inbox_metrics__observe_handler_duration__updates_histogram() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_dur"))

    # Act
    metrics.observe_handler_duration(0.25, event_type="ev")

    # Assert
    hist = metrics._handler_duration_seconds.labels(event_type="ev")
    assert hist._sum.get() == pytest.approx(0.25)  # type: ignore[attr-defined]


def test__prometheus_inbox_metrics__observe_handler_duration_no_label__uses_unknown() -> None:
    # Arrange
    metrics = PrometheusInboxMetrics(prefix=_unique_prefix("in_dur_def"))

    # Act
    metrics.observe_handler_duration(1.0)

    # Assert
    hist = metrics._handler_duration_seconds.labels(event_type="unknown")
    assert hist._sum.get() == pytest.approx(1.0)  # type: ignore[attr-defined]


# -------- import fallback (module-level except branch) --------


def test__prometheus_module__import_fallback__sets_has_prometheus_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force-reimport the module with ``prometheus_client`` blocked to cover the
    ImportError branch.
    """
    # Arrange
    real_pc = sys.modules.pop("prometheus_client", None)
    sys.modules["prometheus_client"] = None  # type: ignore[assignment]

    try:
        # Act
        reloaded = importlib.reload(prom_module)

        # Assert
        assert reloaded._HAS_PROMETHEUS is False
        assert reloaded.Counter is None
        assert reloaded.Gauge is None
        assert reloaded.Histogram is None
    finally:
        # Restore real prometheus_client and reload to leave a clean state.
        if real_pc is not None:
            sys.modules["prometheus_client"] = real_pc
        else:
            sys.modules.pop("prometheus_client", None)
        importlib.reload(prom_module)
