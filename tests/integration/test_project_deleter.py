from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from apolo_events_client import (
    Ack,
    EventsClientConfig,
    EventType,
    RecvEvent,
    RecvEvents,
    StreamType,
    Tag,
)
from apolo_events_client.pytest import EventsQueues
from sqlalchemy.ext.asyncio import AsyncEngine

from platform_neuro_flow_api.api import create_app
from platform_neuro_flow_api.config import Config
from platform_neuro_flow_api.storage.base import NotExistsError
from platform_neuro_flow_api.storage.postgres import PostgresStorage
from tests import _TestClientFactory
from tests.unit.test_in_memory_storage import MockDataHelper


@pytest.fixture
def postgres_storage(sqalchemy_engine: AsyncEngine) -> PostgresStorage:
    return PostgresStorage(sqalchemy_engine)


async def test_deleter(
    aiohttp_client: _TestClientFactory,
    config: Config,
    postgres_storage: PostgresStorage,
    events_config: EventsClientConfig,
    events_queues: EventsQueues,
) -> None:
    # NOTE: we don't need mock ProjectDeleter._delete_task_k8s_pod
    # cause tasks here don't have raw_id
    helper = MockDataHelper(postgres_storage)
    config = replace(config, events=events_config)
    app = await create_app(config)
    await aiohttp_client(app)

    task = await postgres_storage.tasks.create(await helper.gen_task_data())
    assert len([t async for t in postgres_storage.tasks.list()]) == 1
    attempt = await postgres_storage.attempts.get(task.attempt_id)
    assert attempt
    bake = await postgres_storage.bakes.get(attempt.bake_id)
    assert bake
    project = await postgres_storage.projects.get(bake.project_id)
    assert project

    await events_queues.outcome.put(
        RecvEvents(
            subscr_id=uuid4(),
            events=[
                RecvEvent(
                    tag=Tag("123"),
                    timestamp=datetime.now(tz=UTC),
                    sender="platform-admin",
                    stream=StreamType("platform-admin"),
                    event_type=EventType("project-remove"),
                    org=project.org_name,
                    cluster=project.cluster,
                    project=project.project_name,
                    user=project.owner,
                ),
            ],
        )
    )
    ev = await events_queues.income.get()

    assert isinstance(ev, Ack)
    assert ev.events[StreamType("platform-admin")] == ["123"]

    assert len([t async for t in postgres_storage.tasks.list()]) == 0
    with pytest.raises(NotExistsError):
        await postgres_storage.attempts.get(attempt.id)
        await postgres_storage.bakes.get(bake.id)
        await postgres_storage.projects.get(project.id)
