"""Unit tests for ``omni_box.core.models.schemas``."""

from __future__ import annotations

from typing import Any

import pytest

from omni_box.core.models.schemas import BaseEventSchema

pytestmark = pytest.mark.unit


# ---------- Registration & resolve ----------


def test__schema_resolve__exact_and_prefix_match__returns_correct_class() -> None:
    # Arrange
    class V1(BaseEventSchema, event_type="resolve.event"):  # type: ignore[call-arg]
        foo: str

        @classmethod
        def schema_version(cls) -> str:
            return "1.0.0"

    class V2(BaseEventSchema, event_type="resolve.event", version_prefix="2"):  # type: ignore[call-arg]
        bar: int

        @classmethod
        def schema_version(cls) -> str:
            return "2.1.0"

    # Act / Assert
    assert BaseEventSchema.resolve("resolve.event", "1.0.0") is V1
    assert BaseEventSchema.resolve("resolve.event", "2.1.0") is V2
    assert BaseEventSchema.resolve("resolve.event", "2.0.0") is V2
    assert BaseEventSchema.resolve("resolve.event", "2") is V2


def test__schema_resolve__no_event_type__raises_value_error() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="No schemas registered"):
        BaseEventSchema.resolve("unknown.event", "1.0.0")


def test__schema_resolve__no_version__raises_value_error() -> None:
    # Arrange
    class S(BaseEventSchema, event_type="resolve.noversion"):  # type: ignore[call-arg]
        @classmethod
        def schema_version(cls) -> str:
            return "1.0.0"

    # Act / Assert
    with pytest.raises(ValueError, match="No schema_version provided"):
        BaseEventSchema.resolve("resolve.noversion", None)


def test__schema_resolve__unknown_version__raises_value_error() -> None:
    # Arrange
    class S(BaseEventSchema, event_type="resolve.unknownv"):  # type: ignore[call-arg]
        @classmethod
        def schema_version(cls) -> str:
            return "1.0.0"

    # Act / Assert
    with pytest.raises(ValueError, match="Unsupported version"):
        BaseEventSchema.resolve("resolve.unknownv", "9.9.9")


def test__schema_subclass__no_event_type__skips_registration() -> None:
    # Arrange: declare an abstract-ish subclass without event_type kwarg.
    class Anonymous(BaseEventSchema):
        @classmethod
        def schema_version(cls) -> str:
            return "1.0.0"

    # Act / Assert: nothing registered for an empty event_type.
    with pytest.raises(ValueError, match="No schemas registered"):
        BaseEventSchema.resolve("", "1.0.0")


# ---------- to_payload / from_payload ----------


def test__schema_payload__roundtrip__preserves_fields() -> None:
    # Arrange
    class RT(BaseEventSchema, event_type="roundtrip.event"):  # type: ignore[call-arg]
        foo: str

        @classmethod
        def schema_version(cls) -> str:
            return "1.0.0"

    instance = RT(foo="bar")

    # Act
    payload = instance.to_payload()
    restored = RT.from_payload(payload)

    # Assert
    assert payload == {"foo": "bar"}
    assert restored.foo == "bar"


# ---------- Migrations ----------


def test__schema_migrate__same_version__returns_payload_unchanged() -> None:
    # Arrange
    payload = {"x": 1}

    # Act
    result = BaseEventSchema.migrate_payload("mig.event", payload, "1.0.0", "1.0.0")

    # Assert
    assert result is payload


def test__schema_migrate__no_from_version__returns_payload_unchanged() -> None:
    # Arrange
    payload = {"x": 1}

    # Act
    result = BaseEventSchema.migrate_payload("mig.event", payload, None, "1.0.0")

    # Assert
    assert result is payload


def test__schema_migrate__no_migrations_registered__returns_payload_unchanged() -> None:
    # Arrange
    payload = {"x": 1}

    # Act
    result = BaseEventSchema.migrate_payload("mig.notregistered", payload, "1.0.0", "2.0.0")

    # Assert
    assert result is payload


def test__schema_migrate__direct_migration__applies_func() -> None:
    # Arrange
    def upgrade(p: dict[str, Any]) -> dict[str, Any]:
        return {**p, "added": True}

    BaseEventSchema.register_migration("mig.direct", "1.0.0", "2.0.0", upgrade)

    # Act
    result = BaseEventSchema.migrate_payload("mig.direct", {"x": 1}, "1.0.0", "2.0.0")

    # Assert
    assert result == {"x": 1, "added": True}


def test__schema_migrate__multistep_bfs__chains_migrations() -> None:
    # Arrange
    def to_v2(p: dict[str, Any]) -> dict[str, Any]:
        return {**p, "v2": True}

    def to_v3(p: dict[str, Any]) -> dict[str, Any]:
        return {**p, "v3": True}

    BaseEventSchema.register_migration("mig.bfs", "1.0.0", "2.0.0", to_v2)
    BaseEventSchema.register_migration("mig.bfs", "2.0.0", "3.0.0", to_v3)

    # Act
    result = BaseEventSchema.migrate_payload("mig.bfs", {"x": 1}, "1.0.0", "3.0.0")

    # Assert
    assert result == {"x": 1, "v2": True, "v3": True}


def test__schema_migrate__no_path_found__returns_original_payload() -> None:
    # Arrange
    def to_v2(p: dict[str, Any]) -> dict[str, Any]:
        return {**p, "v2": True}

    BaseEventSchema.register_migration("mig.nopath", "1.0.0", "2.0.0", to_v2)
    payload = {"x": 1}

    # Act: there is no path from 1.0.0 to 9.9.9.
    result = BaseEventSchema.migrate_payload("mig.nopath", payload, "1.0.0", "9.9.9")

    # Assert: returns original payload (last fallthrough).
    assert result == {"x": 1}
