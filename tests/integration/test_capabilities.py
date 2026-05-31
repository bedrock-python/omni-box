from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from omni_box.core.exceptions import StorageError
from omni_box.core.protocols import (
    OutboxEventRepository,
    SupportsBulkOperations,
    SupportsDistributedLocking,
    SupportsRetentionPolicies,
)
from omni_box.infra.storage.postgres import PostgresOutboxRepository
from tests.models import ConcreteOutboxEvent

pytestmark = pytest.mark.integration


@pytest.mark.integration
@pytest.mark.asyncio
async def test__postgres_outbox_repository__instantiated__implements_all_capability_protocols(
    async_session: AsyncSession,
) -> None:
    # Arrange
    repo = PostgresOutboxRepository(async_session, model_class=ConcreteOutboxEvent)

    # Act / Assert
    assert isinstance(repo, OutboxEventRepository)
    assert isinstance(repo, SupportsBulkOperations)
    assert isinstance(repo, SupportsDistributedLocking)
    assert isinstance(repo, SupportsRetentionPolicies)


@pytest.mark.integration
@pytest.mark.asyncio
async def test__postgres_outbox_repository__sqlalchemy_error_on_get_by_id__wrapped_as_storage_error_with_cause(
    async_session: AsyncSession,
) -> None:
    # Arrange
    mock_session = AsyncMock()
    mock_session.execute.side_effect = SQLAlchemyError("Original error")
    repo_mock = PostgresOutboxRepository(mock_session, model_class=ConcreteOutboxEvent)

    # Act / Assert
    with pytest.raises(StorageError) as exc_info:
        await repo_mock.get_by_id(uuid4())

    assert "Original error" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, SQLAlchemyError)
