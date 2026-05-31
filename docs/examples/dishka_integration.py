"""Example of integrating omni-box with Dishka DI."""

from dishka import Provider, Scope, provide
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from omni_box.core import OmniBoxDomainService
from omni_box.core.protocols import OutboxEventRepository
from omni_box.infra.storage.postgres import OutboxEventDBBase, PostgresOutboxRepository


class Base(DeclarativeBase):
    pass


class ExampleOutboxEvent(Base, OutboxEventDBBase):
    """Concrete outbox model (inherits __tablename__ and __table_args__ from base)."""


class OutboxProvider(Provider):
    @provide(scope=Scope.APP)
    def get_domain_service(self) -> OmniBoxDomainService:
        return OmniBoxDomainService()

    @provide(scope=Scope.REQUEST)
    def get_repository(self, session: AsyncSession) -> OutboxEventRepository:
        return PostgresOutboxRepository(session, model_class=ExampleOutboxEvent)

    # Note: Background jobs should be configured in their own entrypoints
    # (e.g. using a background task runner or a scheduler)
    # where a new session and repository are created per job run.
