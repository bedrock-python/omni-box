"""Unit tests for ``omni_box.core.dispatch.exceptions``."""

from __future__ import annotations

import pytest

from omni_box.core.dispatch.exceptions import DispatcherError, HandlerAlreadyRegisteredError
from omni_box.core.exceptions import OmniBoxError

pytestmark = pytest.mark.unit


def test__dispatcher_error__inherits_omnibox_error() -> None:
    # Act / Assert
    assert issubclass(DispatcherError, OmniBoxError)


def test__handler_already_registered__inherits_dispatcher_error() -> None:
    # Act / Assert
    assert issubclass(HandlerAlreadyRegisteredError, DispatcherError)


def test__handler_already_registered__with_message__preserves_message() -> None:
    # Arrange
    msg = "Handler for users.user.created already registered"

    # Act
    exc = HandlerAlreadyRegisteredError(msg)

    # Assert
    assert str(exc) == msg


def test__handler_already_registered__raisable__can_be_caught_as_base() -> None:
    # Act / Assert
    with pytest.raises(OmniBoxError):
        raise HandlerAlreadyRegisteredError("boom")
