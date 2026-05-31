"""Unit tests for ``omni_box.core.models.enums``."""

from __future__ import annotations

from enum import StrEnum

import pytest

from omni_box.core.models.enums import EventStatus

pytestmark = pytest.mark.unit


def test__event_status__is_str_enum__inherits_strenum() -> None:
    # Act / Assert
    assert issubclass(EventStatus, StrEnum)


@pytest.mark.parametrize(
    ("member", "expected_value"),
    [
        (EventStatus.PENDING, "pending"),
        (EventStatus.COMPLETED, "completed"),
        (EventStatus.FAILED, "failed"),
    ],
    ids=["pending", "completed", "failed"],
)
def test__event_status__member__has_expected_string_value(member: EventStatus, expected_value: str) -> None:
    # Act / Assert
    assert member.value == expected_value
    assert str(member) == expected_value


def test__event_status__all_members__are_unique() -> None:
    # Arrange
    members = list(EventStatus)
    expected_count = 3

    # Act / Assert
    assert len(members) == expected_count
    assert len({m.value for m in members}) == expected_count


def test__event_status__from_string__resolves_member() -> None:
    # Act / Assert
    assert EventStatus("pending") is EventStatus.PENDING
    assert EventStatus("completed") is EventStatus.COMPLETED
    assert EventStatus("failed") is EventStatus.FAILED
