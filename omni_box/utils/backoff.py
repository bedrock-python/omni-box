"""Backoff utilities."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from random import Random
from typing import Literal

from ..core.constants import DEFAULT_BACKOFF_BASE_SECONDS, DEFAULT_BACKOFF_CAP_SECONDS


@dataclass(frozen=True, slots=True)
class ErrorClassification:
    """Result of classifying a publication error."""

    is_transient: bool
    """True if the error is temporary and retryable."""
    count_as_attempt: bool
    """True if this failure should count toward max_delivery_attempts."""


# Shared PRNG instance for jitter. ``random.Random`` (not ``SystemRandom``)
# is sufficient for jitter and avoids per-call OS-entropy syscalls on hot
# retry paths.
_JITTER_RNG = Random()  # noqa: S311


def calculate_backoff_with_jitter(
    attempt: int,
    base_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS,
    cap_seconds: float = DEFAULT_BACKOFF_CAP_SECONDS,
    jitter_mode: Literal["full", "equal", "decorrelated"] = "full",
) -> float:
    """Calculate exponential backoff with jitter.

    Args:
        attempt: Number of attempts (0-indexed, i.e. attempts_made)
        base_seconds: Base delay in seconds
        cap_seconds: Max delay in seconds
        jitter_mode: Jitter mode:
            - 'full': random in [0, delay]
            - 'equal': random in [delay/2, delay]
            - 'decorrelated': AWS-style decorrelated jitter

    Returns:
        Delay in seconds with jitter

    Raises:
        ValueError: if ``attempt`` is negative, ``base_seconds``/``cap_seconds``
            are non-positive, or ``jitter_mode`` is unknown.
    """
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    if base_seconds <= 0:
        raise ValueError("base_seconds must be > 0")
    if cap_seconds <= 0:
        raise ValueError("cap_seconds must be > 0")

    delay = min(cap_seconds, base_seconds * (2**attempt))

    if jitter_mode == "full":
        return float(_JITTER_RNG.uniform(0, delay))
    if jitter_mode == "equal":
        return float(delay / 2 + _JITTER_RNG.uniform(0, delay / 2))
    if jitter_mode == "decorrelated":
        return float(min(cap_seconds, _JITTER_RNG.uniform(base_seconds, delay * 3)))
    raise ValueError(f"Unknown jitter_mode: {jitter_mode!r}")


class ErrorClassifier:
    """Classifier of publication errors to determine transient vs permanent.

    Transient errors: temporary failures, should not count as attempts.
    Permanent errors: permanent failures (e.g. invalid data), should count as attempts.

    Note:
        ``OSError`` is intentionally *not* in ``TRANSIENT_ERRORS`` because it
        covers a very broad range (e.g. permission errors, disk full) that are
        not necessarily retryable. Only its socket-related subclasses
        (``ConnectionError`` and ``TimeoutError``) are treated as transient.
        Brokers should add their own transient exception types via
        ``additional_transient`` if needed (e.g. ``aiokafka.errors.KafkaError``).
    """

    TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
        TimeoutError,
        asyncio.TimeoutError,
        ConnectionError,
    )

    PERMANENT_ERRORS: tuple[type[BaseException], ...] = (
        ValueError,  # invalid topic, serialization issues
        KeyError,  # missing required fields
        TypeError,  # type mismatch in payload
    )

    @classmethod
    def classify(
        cls,
        exc: BaseException,
        *,
        additional_transient: tuple[type[BaseException], ...] = (),
        additional_permanent: tuple[type[BaseException], ...] = (),
    ) -> ErrorClassification:
        """Classify exception and determine handling strategy.

        Args:
            exc: Exception to classify
            additional_transient: Extra exception types that should be treated
                as transient (e.g. broker-specific connection errors).
            additional_permanent: Extra exception types that should be treated
                as permanent (e.g. ``RecordTooLargeError``).

        Returns:
            ErrorClassification with is_transient and count_as_attempt.
        """
        if isinstance(exc, (*cls.TRANSIENT_ERRORS, *additional_transient)):
            return ErrorClassification(is_transient=True, count_as_attempt=False)
        if isinstance(exc, (*cls.PERMANENT_ERRORS, *additional_permanent)):
            return ErrorClassification(is_transient=False, count_as_attempt=True)
        # Unknown error — conservative: treat as permanent
        return ErrorClassification(is_transient=False, count_as_attempt=True)
