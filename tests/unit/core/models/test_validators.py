"""Unit tests for ``omni_box.core.models.validators``."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationInfo

from omni_box.core.models.validators import validate_headers, validate_payload

pytestmark = pytest.mark.unit


def _info(context: dict[str, Any] | None = None) -> ValidationInfo:
    info = MagicMock(spec=ValidationInfo)
    info.context = context if context is not None else {}
    return info  # type: ignore[no-any-return]


# ---------- validate_payload ----------


def test__validate_payload__valid_dict__returns_normalized_dict() -> None:
    # Arrange
    payload = {"foo": "bar", "num": 123}

    # Act
    result = validate_payload(payload, _info())

    # Assert
    assert result == payload


def test__validate_payload__not_a_dict__raises_type_error() -> None:
    # Act / Assert
    with pytest.raises(TypeError, match="payload must be a JSON object"):
        validate_payload([1, 2, 3], _info())  # type: ignore[arg-type]


def test__validate_payload__empty_dict__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="payload cannot be empty"):
        validate_payload({}, _info())


def test__validate_payload__trusted_context__skips_size_check() -> None:
    # Arrange
    big = {"large": "x" * 2_000_000}

    # Act
    result = validate_payload(big, _info({"payload_trusted": True}))

    # Assert
    assert result is big


def test__validate_payload__exceeds_max_bytes__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="Payload size exceeds"):
        validate_payload({"foo": "a" * 20}, _info({"payload_max_bytes": 10}))


def test__validate_payload__invalid_max_bytes__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="payload_max_bytes must be >= 1"):
        validate_payload({"f": 1}, _info({"payload_max_bytes": 0}))


def test__validate_payload__nan_value__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="non-JSON-serializable"):
        validate_payload({"foo": float("nan")}, _info())


def test__validate_payload__non_serializable_object__raises_value_error() -> None:
    # Arrange: a set is not JSON-serializable by orjson.
    payload: dict[str, Any] = {"weird": {1, 2}}

    # Act / Assert
    with pytest.raises(ValueError, match="non-JSON-serializable"):
        validate_payload(payload, _info())


def test__validate_payload__default_context_none__uses_module_defaults() -> None:
    # Arrange: pass an info-like object whose .context is None.
    info = MagicMock(spec=ValidationInfo)
    info.context = None

    # Act
    result = validate_payload({"k": "v"}, info)

    # Assert
    assert result == {"k": "v"}


# ---------- validate_headers ----------


def test__validate_headers__none__returns_none() -> None:
    # Act / Assert
    assert validate_headers(None, _info()) is None


def test__validate_headers__empty_dict__returns_none() -> None:
    # Act / Assert
    assert validate_headers({}, _info()) is None


def test__validate_headers__padded_keys_and_values__strips_and_returns() -> None:
    # Act
    result = validate_headers({" Key ": " Value "}, _info())

    # Assert
    assert result == {"Key": "Value"}


def test__validate_headers__empty_key__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="Header key cannot be empty"):
        validate_headers({"   ": "val"}, _info())


def test__validate_headers__empty_value__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="Header value for 'K' cannot be empty"):
        validate_headers({"K": "   "}, _info())


def test__validate_headers__control_char_in_key__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="contains control characters"):
        validate_headers({"Key\x01Name": "val"}, _info())


def test__validate_headers__control_char_in_value__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="contains control characters"):
        validate_headers({"Key": "val\x00data"}, _info())


def test__validate_headers__tab_in_value__is_allowed() -> None:
    # Act
    result = validate_headers({"Key": "with\ttab"}, _info())

    # Assert
    assert result == {"Key": "with\ttab"}


def test__validate_headers__key_too_long__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="is too long"):
        validate_headers({"Toolong": "val"}, _info({"header_key_max_length": 5}))


def test__validate_headers__value_too_long__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="Header value for 'K' is too long"):
        validate_headers({"K": "abcdef"}, _info({"header_value_max_length": 3}))


def test__validate_headers__case_insensitive_duplicate__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="conflicts with another key"):
        validate_headers({"Key": "v1", "KEY": "v2"}, _info())


def test__validate_headers__exact_duplicate_after_strip__raises_value_error() -> None:
    # Arrange: two keys whose stripped form is identical.
    headers = {" Key": "v1", "Key ": "v2"}

    # Act / Assert
    with pytest.raises(ValueError, match="Duplicate header key after normalization"):
        validate_headers(headers, _info())


def test__validate_headers__too_many__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="Too many headers"):
        validate_headers({"h1": "v1", "h2": "v2"}, _info({"headers_max_count": 1}))


@pytest.mark.parametrize(
    ("ctx", "match"),
    [
        ({"headers_max_count": -1}, "headers_max_count must be >= 0"),
        ({"header_key_max_length": 0}, "header_key_max_length must be >= 1"),
        ({"header_value_max_length": 0}, "header_value_max_length must be >= 1"),
    ],
    ids=["max-count-negative", "key-max-zero", "value-max-zero"],
)
def test__validate_headers__invalid_limits__raises_value_error(ctx: dict[str, int], match: str) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=match):
        validate_headers({"h": "v"}, _info(ctx))


def test__validate_headers__context_none__uses_defaults() -> None:
    # Arrange
    info = MagicMock(spec=ValidationInfo)
    info.context = None

    # Act
    result = validate_headers({"h": "v"}, info)

    # Assert
    assert result == {"h": "v"}
