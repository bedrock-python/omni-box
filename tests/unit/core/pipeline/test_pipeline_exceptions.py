"""Unit tests for ``omni_box.core.pipeline.exceptions``."""

from __future__ import annotations

import pytest

from omni_box.core.pipeline.exceptions import PipelineError, PipelineStoppedError

pytestmark = pytest.mark.unit


def test__pipeline_error__class__is_exception_subclass() -> None:
    # Arrange / Act / Assert
    assert issubclass(PipelineError, Exception)


def test__pipeline_stopped_error__class__is_pipeline_error_subclass() -> None:
    # Arrange / Act / Assert
    assert issubclass(PipelineStoppedError, PipelineError)


def test__pipeline_stopped_error__raised_with_message__preserves_message() -> None:
    # Arrange
    message = "stop now"

    # Act
    with pytest.raises(PipelineStoppedError) as exc_info:
        raise PipelineStoppedError(message)

    # Assert
    assert str(exc_info.value) == message


def test__pipeline_stopped_error__raised__catchable_as_pipeline_error() -> None:
    # Arrange / Act / Assert
    with pytest.raises(PipelineError):
        raise PipelineStoppedError
