from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from ...constants import REASON_DUPLICATE_SIBLING
from ...protocols.repository import InboxEventRepository
from ..step import BaseProcessingStep, StepResult

if TYPE_CHECKING:
    from ...models.entities import InboxEvent
    from ..context import ProcessingContext

logger = structlog.get_logger(__name__)


class SiblingDeduplicationStep(BaseProcessingStep["InboxEvent"]):
    """Inbox-specific step that skips an event if a sibling event is already completed."""

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled

    async def execute(
        self,
        event: InboxEvent,
        context: ProcessingContext[InboxEvent],
    ) -> StepResult:
        """Check for completed siblings and skip if found."""
        if not self.enabled:
            return StepResult.next()

        repo = context.repo
        if not isinstance(repo, InboxEventRepository):
            raise TypeError(
                "SiblingDeduplicationStep requires InboxEventRepository "
                f"(has_completed_sibling_for_inbox_key), got {type(repo).__name__}"
            )

        has_sibling = await repo.has_completed_sibling_for_inbox_key(
            event.message_id,
            event.consumer_group,
            event.id,
        )

        if has_sibling:
            context.mark_skipped(event.id, REASON_DUPLICATE_SIBLING)
            return StepResult.skip()

        return StepResult.next()
