from unittest.mock import AsyncMock, MagicMock

import pytest

from omni_box.application.factories import (
    create_dispatching_processor,
    create_inbox_processor,
    create_outbox_processor,
)
from omni_box.core.dispatch.registry import EventRouter
from omni_box.core.services.processor import EventBatchProcessor

pytestmark = pytest.mark.unit


def test__create_inbox_processor__with_handler__returns_processor_with_two_steps() -> None:
    # Arrange
    repo = MagicMock()
    handler = AsyncMock()

    # Act
    processor = create_inbox_processor(repo=repo, handler=handler, job_name="my_inbox_processor")

    # Assert
    assert isinstance(processor, EventBatchProcessor)
    assert processor._job_name == "my_inbox_processor"
    # SiblingDeduplicationStep, HandlerExecutionStep
    assert len(processor._pipeline._steps) == 2


def test__create_outbox_processor__with_publisher__returns_processor_with_one_step() -> None:
    # Arrange
    repo = MagicMock()
    publisher = MagicMock()

    # Act
    processor = create_outbox_processor(repo=repo, publisher=publisher, job_name="my_outbox_processor")

    # Assert
    assert isinstance(processor, EventBatchProcessor)
    assert processor._job_name == "my_outbox_processor"
    # HandlerExecutionStep
    assert len(processor._pipeline._steps) == 1


@pytest.mark.asyncio
async def test__create_dispatching_processor__with_router__returns_processor_with_two_steps() -> None:
    # Arrange
    repo = MagicMock()
    router = MagicMock(spec=EventRouter)

    # Act
    processor = create_dispatching_processor(repo=repo, router=router, job_name="my_dispatch_processor")

    # Assert
    assert isinstance(processor, EventBatchProcessor)
    assert processor._job_name == "my_dispatch_processor"
    # SiblingDeduplicationStep, HandlerExecutionStep
    assert len(processor._pipeline._steps) == 2
