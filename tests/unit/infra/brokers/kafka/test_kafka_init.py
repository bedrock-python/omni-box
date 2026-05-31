"""Unit tests for the ``omni_box.infra.brokers.kafka`` package init.

The module re-exports adapter classes from submodules but tolerates ``ImportError``
when the optional ``aiokafka`` dependency is missing. This file verifies both the
happy import path and the fallback path.
"""

from __future__ import annotations

import builtins
import importlib
import sys

import pytest

import omni_box.infra.brokers.kafka as kafka_pkg

pytestmark = pytest.mark.unit


# ---------- happy path ----------


def test__kafka_init__aiokafka_available__exposes_publisher_and_consumer() -> None:
    # Arrange
    # Act
    importlib.reload(kafka_pkg)

    # Assert
    assert hasattr(kafka_pkg, "KafkaEventPublisher")
    assert hasattr(kafka_pkg, "KafkaEventConsumer")


# ---------- ImportError fallback ----------


def test__kafka_init__aiokafka_missing__import_errors_are_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange: drop cached submodules + force re-import errors on aiokafka.
    for mod in [
        "omni_box.infra.brokers.kafka",
        "omni_box.infra.brokers.kafka.consumer",
        "omni_box.infra.brokers.kafka.publisher",
        "aiokafka",
    ]:
        monkeypatch.delitem(sys.modules, mod, raising=False)

    real_import = builtins.__import__

    def _import(name: str, *args: object, **kwargs: object) -> object:
        if name == "aiokafka" or name.startswith("aiokafka."):
            raise ImportError("aiokafka is not installed")
        return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(builtins, "__import__", _import)

    # Act
    pkg = importlib.import_module("omni_box.infra.brokers.kafka")

    # Assert: package loads even without aiokafka; adapters are not re-exported.
    assert not hasattr(pkg, "KafkaEventPublisher")
    assert not hasattr(pkg, "KafkaEventConsumer")
