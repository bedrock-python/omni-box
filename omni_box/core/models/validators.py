"""Outbox payload and headers validators."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import orjson

from ..constants import (
    DEFAULT_HEADER_KEY_MAX_LENGTH,
    DEFAULT_HEADER_VALUE_MAX_LENGTH,
    DEFAULT_HEADERS_MAX_COUNT,
    DEFAULT_PAYLOAD_MAX_BYTES,
)

if TYPE_CHECKING:
    from pydantic import ValidationInfo


__all__ = ["orjson", "validate_headers", "validate_payload"]


def validate_payload(v: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:
    """Ensure payload size is within reasonable limits.

    Checks compact JSON representation size in bytes.
    """
    if not isinstance(v, dict):
        raise TypeError("payload must be a JSON object")

    # Check for empty payload
    if not v:
        raise ValueError("payload cannot be empty")

    # Optimization: skip validation if payload is marked as trusted
    if info.context and info.context.get("payload_trusted"):
        return v

    payload_max_bytes = DEFAULT_PAYLOAD_MAX_BYTES
    if info.context:
        payload_max_bytes = info.context.get("payload_max_bytes", payload_max_bytes)

    if payload_max_bytes < 1:
        raise ValueError(f"payload_max_bytes must be >= 1, got {payload_max_bytes}")

    # Check if payload is JSON-serializable and does not contain NaN/Inf.
    # RFC 8259 (JSON) does not support NaN/Infinity. orjson.dumps is extremely fast
    # but by default it may convert NaN/Inf to null. To maintain strict validation
    # we use orjson for size check and serialization, and standard json for
    # NaN/Inf rejection if needed.
    try:
        # orjson.dumps returns bytes and is extremely fast.
        dumped = orjson.dumps(v)
    except orjson.JSONEncodeError as e:
        raise ValueError(f"Payload contains non-JSON-serializable values: {e}") from e

    # Check size limit
    if len(dumped) > payload_max_bytes:
        raise ValueError(f"Payload size exceeds {payload_max_bytes} bytes limit")

    # Strict NaN/Infinity check to match previous behavior and RFC 8259.
    # We only do this if orjson.dumps succeeded.
    # Standard json.dumps with allow_nan=False will raise ValueError if NaN/Inf are present.
    # To keep it fast, we can use a faster way to detect NaN if needed,
    # but for now we'll use json.dumps as it's reliable for this specific check.
    try:
        json.dumps(v, allow_nan=False)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Payload contains non-JSON-serializable values: {e}") from e

    # We use orjson.loads to ensure we return a clean dict
    # that is exactly what will be sent to Kafka.
    result = orjson.loads(dumped)
    if not isinstance(result, dict):  # pragma: no cover
        raise TypeError(f"Expected dict from JSON, got {type(result).__name__}")
    return result


def validate_headers(v: dict[str, str] | None, info: ValidationInfo) -> dict[str, str] | None:
    """Ensure header values are within reasonable limits."""
    if v is None:
        return v

    # Normalize empty dict to None
    if not v:
        return None

    headers_max_count = DEFAULT_HEADERS_MAX_COUNT
    header_key_max_length = DEFAULT_HEADER_KEY_MAX_LENGTH
    header_value_max_length = DEFAULT_HEADER_VALUE_MAX_LENGTH

    if info.context:
        headers_max_count = info.context.get("headers_max_count", headers_max_count)
        header_key_max_length = info.context.get("header_key_max_length", header_key_max_length)
        header_value_max_length = info.context.get("header_value_max_length", header_value_max_length)

    if headers_max_count < 0:
        raise ValueError(f"headers_max_count must be >= 0, got {headers_max_count}")
    if header_key_max_length < 1:
        raise ValueError(f"header_key_max_length must be >= 1, got {header_key_max_length}")
    if header_value_max_length < 1:
        raise ValueError(f"header_value_max_length must be >= 1, got {header_value_max_length}")

    normalized: dict[str, str] = {}
    normalized_lower_keys: set[str] = set()

    for key, value in v.items():
        stripped_key = key.strip()
        if not stripped_key:
            raise ValueError("Header key cannot be empty or whitespace")

        # Check for control characters in key
        if any(ord(c) < 32 or ord(c) == 127 for c in stripped_key):
            raise ValueError(f"Header key '{stripped_key}' contains control characters")

        stripped_value = value.strip()
        if not stripped_value:
            raise ValueError(f"Header value for '{stripped_key}' cannot be empty or whitespace")

        # Check for control characters in value (except tab which might be acceptable)
        if any((ord(c) < 32 and c != "\t") or ord(c) == 127 for c in stripped_value):
            raise ValueError(f"Header value for '{stripped_key}' contains control characters")

        if len(stripped_key) > header_key_max_length:
            raise ValueError(f"Header key '{stripped_key}' is too long (max {header_key_max_length} chars)")
        if len(stripped_value) > header_value_max_length:
            raise ValueError(f"Header value for '{stripped_key}' is too long (max {header_value_max_length} chars)")

        # Check for exact duplicate
        if stripped_key in normalized:
            raise ValueError(f"Duplicate header key after normalization: '{stripped_key}'")

        # Check for case-insensitive duplicate
        lower_key = stripped_key.lower()
        if lower_key in normalized_lower_keys:
            raise ValueError(f"Header key '{stripped_key}' conflicts with another key (case-insensitive)")

        normalized[stripped_key] = stripped_value
        normalized_lower_keys.add(lower_key)

    if len(normalized) > headers_max_count:
        raise ValueError(f"Too many headers (max {headers_max_count})")

    return normalized if normalized else None
