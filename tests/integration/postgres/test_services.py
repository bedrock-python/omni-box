from uuid import UUID, uuid4

import pytest

from omni_box.core.services.domain import OmniBoxDomainService

pytestmark = pytest.mark.integration


def test__omni_box_domain_service__create_outbox_event__returns_event_with_correct_fields() -> None:
    # Arrange
    service = OmniBoxDomainService()
    agg_id = uuid4()

    # Act
    event = service.create_outbox_event(
        aggregate_type="order",
        aggregate_id=agg_id,
        event_type="order.created",
        topic="orders",
        partition_key="key",
        payload={"foo": "bar"},
    )

    # Assert
    assert event.aggregate_id == agg_id
    assert event.aggregate_type == "order"
    assert event.payload == {"foo": "bar"}
    assert isinstance(event.id, UUID)
