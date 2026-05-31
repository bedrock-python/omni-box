"""Unit tests for ``omni_box.utils.backoff``."""

from __future__ import annotations

import pytest

from omni_box.core.constants import DEFAULT_BACKOFF_BASE_SECONDS, DEFAULT_BACKOFF_CAP_SECONDS
from omni_box.utils.backoff import ErrorClassification, ErrorClassifier, calculate_backoff_with_jitter

pytestmark = pytest.mark.unit


# ---------- calculate_backoff_with_jitter ----------


@pytest.mark.parametrize(
    "attempt",
    [0, 1, 2, 5, 10, 30],
    ids=["attempt-0", "attempt-1", "attempt-2", "attempt-5", "attempt-10", "attempt-30"],
)
def test__calculate_backoff__full_jitter__delay_within_bounds(attempt: int) -> None:
    # Arrange
    base = 1.0
    cap = 60.0
    expected_max = min(cap, base * (2**attempt))

    # Act
    delay = calculate_backoff_with_jitter(attempt, base_seconds=base, cap_seconds=cap, jitter_mode="full")

    # Assert
    assert 0.0 <= delay <= expected_max


def test__calculate_backoff__equal_jitter__delay_between_half_and_full_window() -> None:
    # Arrange
    attempt = 3
    base = 1.0
    cap = 100.0
    window = min(cap, base * (2**attempt))

    # Act
    delay = calculate_backoff_with_jitter(attempt, base_seconds=base, cap_seconds=cap, jitter_mode="equal")

    # Assert
    assert window / 2 <= delay <= window


def test__calculate_backoff__decorrelated_jitter__never_exceeds_cap() -> None:
    # Arrange
    attempt = 10
    base = 1.0
    cap = 30.0

    # Act
    delay = calculate_backoff_with_jitter(attempt, base_seconds=base, cap_seconds=cap, jitter_mode="decorrelated")

    # Assert
    assert base <= delay <= cap


def test__calculate_backoff__large_attempt__delay_capped_at_cap_seconds() -> None:
    # Arrange
    attempt = 100  # 2**100 * base would explode without the cap
    base = 1.0
    cap = 5.0

    # Act
    delay = calculate_backoff_with_jitter(attempt, base_seconds=base, cap_seconds=cap, jitter_mode="full")

    # Assert
    assert 0.0 <= delay <= cap


def test__calculate_backoff__no_kwargs__uses_module_defaults() -> None:
    # Act
    delay = calculate_backoff_with_jitter(attempt=0)

    # Assert
    assert 0.0 <= delay <= DEFAULT_BACKOFF_BASE_SECONDS
    assert DEFAULT_BACKOFF_CAP_SECONDS > DEFAULT_BACKOFF_BASE_SECONDS


# ---------- ErrorClassification dataclass ----------


def test__error_classification__instance__is_immutable() -> None:
    # Arrange
    cls = ErrorClassification(is_transient=True, count_as_attempt=False)

    # Act / Assert
    with pytest.raises(AttributeError):
        cls.is_transient = False  # type: ignore[misc]


# ---------- ErrorClassifier.classify ----------


@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError("timed out"),
        TimeoutError(),
        ConnectionError("conn lost"),
    ],
    ids=["TimeoutError", "asyncio.TimeoutError", "ConnectionError"],
)
def test__error_classifier__transient_error__marked_transient_and_not_counted(exc: Exception) -> None:
    # Act
    result = ErrorClassifier.classify(exc)

    # Assert
    assert result.is_transient is True
    assert result.count_as_attempt is False


def test__error_classifier__plain_oserror__treated_as_permanent_by_default() -> None:
    # ``OSError`` covers far more than network failures (e.g. EACCES, ENOSPC),
    # so it must NOT be classified as transient out of the box. Callers can
    # opt in via ``additional_transient`` when they know the context.
    result = ErrorClassifier.classify(OSError("network is unreachable"))

    assert result.is_transient is False
    assert result.count_as_attempt is True


def test__error_classifier__additional_transient_override__overrides_default() -> None:
    class _BrokerConnError(Exception):
        pass

    result = ErrorClassifier.classify(_BrokerConnError(), additional_transient=(_BrokerConnError,))

    assert result.is_transient is True
    assert result.count_as_attempt is False


def test__error_classifier__additional_permanent_override__overrides_default() -> None:
    class _RecordTooLargeError(Exception):
        pass

    result = ErrorClassifier.classify(_RecordTooLargeError(), additional_permanent=(_RecordTooLargeError,))

    assert result.is_transient is False
    assert result.count_as_attempt is True


@pytest.mark.parametrize(
    "exc",
    [ValueError("bad payload"), KeyError("missing"), TypeError("wrong type")],
    ids=["ValueError", "KeyError", "TypeError"],
)
def test__error_classifier__permanent_error__marked_permanent_and_counted(exc: Exception) -> None:
    # Act
    result = ErrorClassifier.classify(exc)

    # Assert
    assert result.is_transient is False
    assert result.count_as_attempt is True


def test__error_classifier__unknown_error__treated_as_permanent_and_counted() -> None:
    # Arrange
    class CustomError(Exception):
        pass

    # Act
    result = ErrorClassifier.classify(CustomError("boom"))

    # Assert
    assert result.is_transient is False
    assert result.count_as_attempt is True
