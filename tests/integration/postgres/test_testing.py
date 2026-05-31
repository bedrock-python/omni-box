from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from omni_box.core.models.enums import EventStatus
from omni_box.testing import assert_outbox_event_created

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test__assert_outbox_event_created__event_exists_with_matching_status__returns_event() -> None:
    # Arrange
    repo = AsyncMock()
    agg_id = uuid4()
    mock_event = MagicMock()
    mock_event.aggregate_id = agg_id
    mock_event.event_type = "test"
    mock_event.status = EventStatus.PENDING
    repo.fetch_pending.return_value = [mock_event]

    # Act
    result = await assert_outbox_event_created(repo, agg_id, "test")

    # Assert
    assert result == mock_event


@pytest.mark.asyncio
async def test__assert_outbox_event_created__no_matching_event__raises_assertion_error() -> None:
    # Arrange
    repo = AsyncMock()
    repo.fetch_pending.return_value = []

    # Act / Assert
    with pytest.raises(AssertionError, match="not found"):
        await assert_outbox_event_created(repo, uuid4(), "test")


@pytest.mark.asyncio
async def test__assert_outbox_event_created__event_has_wrong_status__raises_assertion_error() -> None:
    # Arrange
    repo = AsyncMock()
    agg_id = uuid4()
    mock_event = MagicMock()
    mock_event.aggregate_id = agg_id
    mock_event.event_type = "test"
    mock_event.status = EventStatus.COMPLETED
    repo.fetch_pending.return_value = [mock_event]

    # Act / Assert
    with pytest.raises(AssertionError, match="Expected status"):
        await assert_outbox_event_created(repo, agg_id, "test", status=EventStatus.PENDING)
