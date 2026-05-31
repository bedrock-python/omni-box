"""Unit tests for ``omni_box.contrib.settings``."""

from __future__ import annotations

import pytest

from omni_box.contrib.settings import (
    BaseInboxSettings,
    BaseOutboxSettings,
    OmniBoxObservabilitySettings,
)

pytestmark = pytest.mark.unit


def test__base_inbox_settings__defaults__match_field_definitions() -> None:
    # Arrange / Act
    settings = BaseInboxSettings()

    # Assert
    assert settings.processor.batch_size == 50
    assert settings.processor.interval_seconds == 5
    assert settings.maintenance.stale_lock_timeout_seconds == 300
    assert settings.resilience.enable_circuit_breaker is True
    assert settings.observability.enable_otel is False
    assert settings.observability.enable_metrics is True
    assert settings.skip_duplicate_siblings is True


def test__base_outbox_settings__defaults__match_field_definitions() -> None:
    # Arrange / Act
    settings = BaseOutboxSettings()

    # Assert
    assert settings.processor.max_delivery_attempts == 5
    assert settings.maintenance.retention_days is None
    assert settings.resilience.failure_threshold == 5


def test__omni_box_observability_settings__otel_disabled_by_default() -> None:
    # ``enable_otel`` must default to False so a host that only installed
    # ``opentelemetry-api`` (no exporter) does not silently no-op tracing.
    assert OmniBoxObservabilitySettings().enable_otel is False


def test__base_outbox_settings__env_prefix__loads_nested_field(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setenv("OMNI_OUTBOX_PROCESSOR__BATCH_SIZE", "200")
    monkeypatch.setenv("OMNI_OUTBOX_OBSERVABILITY__ENABLE_OTEL", "true")

    # Act
    settings = BaseOutboxSettings()

    # Assert
    assert settings.processor.batch_size == 200
    assert settings.observability.enable_otel is True


def test__base_inbox_settings__env_prefix__loads_nested_field(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setenv("OMNI_INBOX_PROCESSOR__INTERVAL_SECONDS", "30")
    monkeypatch.setenv("OMNI_INBOX_SKIP_DUPLICATE_SIBLINGS", "false")

    # Act
    settings = BaseInboxSettings()

    # Assert
    assert settings.processor.interval_seconds == 30
    assert settings.skip_duplicate_siblings is False


def test__base_outbox_settings__unknown_env__ignored_silently(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    monkeypatch.setenv("OMNI_OUTBOX_NOT_A_FIELD", "boom")

    # Act / Assert (must not raise even though the var has the right prefix)
    BaseOutboxSettings()


def test__base_outbox_settings__inbox_prefix_does_not_leak(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: only OMNI_INBOX_ should not influence outbox settings.
    monkeypatch.setenv("OMNI_INBOX_PROCESSOR__BATCH_SIZE", "999")

    # Act
    outbox = BaseOutboxSettings()

    # Assert
    assert outbox.processor.batch_size == 50
