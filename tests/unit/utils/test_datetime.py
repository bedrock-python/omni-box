"""Unit tests for ``omni_box.utils.datetime``."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from omni_box.utils.datetime import is_naive, utc_now

pytestmark = pytest.mark.unit


# ---------- utc_now ----------


def test__utc_now__returned_value__is_timezone_aware_in_utc() -> None:
    # Act
    now = utc_now()

    # Assert
    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)


def test__utc_now__two_successive_calls__second_not_earlier_than_first() -> None:
    # Act
    first = utc_now()
    second = utc_now()

    # Assert
    assert second >= first


# ---------- is_naive ----------


def test__is_naive__naive_datetime__returns_true() -> None:
    # Arrange
    naive = datetime(2026, 1, 1, 12, 0, 0)

    # Act / Assert
    assert is_naive(naive) is True


@pytest.mark.parametrize(
    "tz",
    [UTC, timezone(timedelta(hours=3)), timezone(timedelta(hours=-5))],
    ids=["utc", "msk", "est"],
)
def test__is_naive__aware_datetime__returns_false(tz: timezone) -> None:
    # Arrange
    aware = datetime(2026, 1, 1, 12, 0, 0, tzinfo=tz)

    # Act / Assert
    assert is_naive(aware) is False


def test__is_naive__tzinfo_returning_none_utcoffset__returns_true() -> None:
    # Arrange
    class _NullTz(timezone.__base__):  # type: ignore[misc]
        def utcoffset(self, dt: datetime | None) -> timedelta | None:
            return None

        def tzname(self, dt: datetime | None) -> str:
            return "null"

        def dst(self, dt: datetime | None) -> timedelta | None:
            return None

    dt = datetime(2026, 1, 1, 12, 0, 0, tzinfo=_NullTz())

    # Act / Assert
    assert is_naive(dt) is True
