"""Unit tests for transaction protocols."""

from __future__ import annotations

import pytest

from omni_box.core.protocols.transaction import InboxTransactionProviderProtocol

pytestmark = pytest.mark.unit


def test__inbox_transaction_provider_protocol__conforming_impl__passes_isinstance_check() -> None:
    # Arrange
    class MockProvider:
        def transaction(self):
            pass

    # Act / Assert
    assert isinstance(MockProvider(), InboxTransactionProviderProtocol)
