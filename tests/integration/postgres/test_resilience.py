from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from omni_box.application.services.publish import OutboxPublisher
from tests.helpers import FakeEventPublisher, FakeLogger, FakeOutboxStore

pytestmark = pytest.mark.integration


@pytest.mark.unit
@pytest.mark.asyncio
async def test__outbox_publisher__db_crash_during_fetch__logs_error_and_re_raises() -> None:
    # Arrange
    outbox = FakeOutboxStore()
    broker = FakeEventPublisher()
    publisher = OutboxPublisher(outbox, broker)
    logger = FakeLogger()

    # Act / Assert
    with (
        patch("omni_box.core.services.processor.logger", logger),
        patch.object(outbox, "fetch_and_lock_pending", side_effect=SQLAlchemyError("DB crash")),
        pytest.raises(SQLAlchemyError),
    ):
        await publisher.publish_batch(worker_id="w1", batch_size=10)

    assert any("outbox_publisher batch failed due to unexpected error" in msg[0] for msg in logger.exception_calls)
