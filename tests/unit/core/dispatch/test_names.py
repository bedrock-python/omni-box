"""Unit tests for ``omni_box.core.dispatch.names``."""

from __future__ import annotations

from enum import StrEnum

import pytest

from omni_box.core.dispatch.names import DispatchName, as_dispatch_str

pytestmark = pytest.mark.unit


class _Topic(StrEnum):
    ORDERS = "orders"
    USERS = "users"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("plain", "plain"),
        (_Topic.ORDERS, "orders"),
        (_Topic.USERS, "users"),
    ],
    ids=["plain-str", "strenum-orders", "strenum-users"],
)
def test__as_dispatch_str__string_or_strenum__returns_string(value: DispatchName, expected: str) -> None:
    # Act
    result = as_dispatch_str(value)

    # Assert
    assert result == expected
    assert isinstance(result, str)
