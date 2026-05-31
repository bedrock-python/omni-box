"""Unit tests for ``omni_box.core.models.types``."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import TypeAdapter

from omni_box.core.models.types import (
    EventFailureUpdate,
    PositiveInt,
    PositiveNumber,
    StrippedNonEmptyStr,
    _strip_and_check_empty,
)

pytestmark = pytest.mark.unit


# ---------- _strip_and_check_empty ----------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (" foo ", "foo"),
        ("bar", "bar"),
        ("\t\nbaz\t", "baz"),
    ],
    ids=["padded", "clean", "tabs-newlines"],
)
def test__strip_and_check_empty__valid_string__returns_stripped(raw: str, expected: str) -> None:
    # Act / Assert
    assert _strip_and_check_empty(raw) == expected


@pytest.mark.parametrize(
    "raw",
    ["", "   ", "\t\n", " "],
    ids=["empty", "spaces", "whitespace", "single-space"],
)
def test__strip_and_check_empty__empty_or_whitespace__raises_value_error(raw: str) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="cannot be empty or whitespace"):
        _strip_and_check_empty(raw)


@pytest.mark.parametrize(
    "raw",
    [123, None, [1, 2], 0.5],
    ids=["int", "none", "list", "float"],
)
def test__strip_and_check_empty__non_string__returns_unchanged(raw: object) -> None:
    # Act / Assert
    assert _strip_and_check_empty(raw) is raw


# ---------- StrippedNonEmptyStr ----------


def test__stripped_non_empty_str__padded_input__strips_whitespace() -> None:
    # Arrange
    adapter = TypeAdapter(StrippedNonEmptyStr)

    # Act
    result = adapter.validate_python(" bar ")

    # Assert
    assert result == "bar"


def test__stripped_non_empty_str__whitespace_only__raises_value_error() -> None:
    # Arrange
    adapter = TypeAdapter(StrippedNonEmptyStr)

    # Act / Assert
    with pytest.raises(ValueError, match="cannot be empty"):
        adapter.validate_python("   ")


# ---------- PositiveInt / PositiveNumber ----------


def test__positive_int__zero__raises_value_error() -> None:
    # Arrange
    adapter = TypeAdapter(PositiveInt)

    # Act / Assert
    with pytest.raises(ValueError):
        adapter.validate_python(0)


def test__positive_int__positive_value__returns_value() -> None:
    # Arrange
    adapter = TypeAdapter(PositiveInt)

    # Act / Assert
    assert adapter.validate_python(5) == 5


def test__positive_number__non_positive__raises_value_error() -> None:
    # Arrange
    adapter = TypeAdapter(PositiveNumber)

    # Act / Assert
    with pytest.raises(ValueError):
        adapter.validate_python(0.0)


def test__positive_number__positive_value__returns_value() -> None:
    # Arrange
    adapter = TypeAdapter(PositiveNumber)

    # Act / Assert
    assert adapter.validate_python(0.5) == 0.5


# ---------- EventFailureUpdate ----------


def test__event_failure_update__default_next_retry_at__is_none() -> None:
    # Arrange
    event_id = uuid4()

    # Act
    update = EventFailureUpdate(event_id=event_id, error="boom")

    # Assert
    assert update.event_id == event_id
    assert update.error == "boom"
    assert update.next_retry_at is None


def test__event_failure_update__with_retry__stores_timestamp() -> None:
    # Arrange
    event_id = uuid4()
    when = datetime.now(UTC)

    # Act
    update = EventFailureUpdate(event_id=event_id, error="err", next_retry_at=when)

    # Assert
    assert update.next_retry_at == when


def test__event_failure_update__namedtuple__is_iterable() -> None:
    # Arrange
    event_id = uuid4()
    update = EventFailureUpdate(event_id=event_id, error="err")

    # Act
    fields = tuple(update)

    # Assert
    assert fields == (event_id, "err", None)
