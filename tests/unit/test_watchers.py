from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock, Mock

import pytest
from apolo_sdk import Client as PlatformClient, JobDescription, JobStatus

from platform_neuro_flow_api.storage.base import AttemptStorage, TaskStatus
from platform_neuro_flow_api.storage.in_memory import InMemoryStorage
from platform_neuro_flow_api.watchers import ExecutorAliveWatcher

from tests.unit.test_in_memory_storage import MockDataHelper
from tests.utils import make_descr


class TestExecutorAliveWatcher:
    helper: MockDataHelper

    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> AttemptStorage:
        return in_memory_storage.attempts

    @pytest.fixture(autouse=True)
    def _helper(self, in_memory_storage: InMemoryStorage) -> None:
        self.helper = MockDataHelper(in_memory_storage)

    @pytest.fixture
    def client_mock(self) -> PlatformClient:
        return Mock()

    @pytest.fixture
    def watcher(
        self, client_mock: PlatformClient, storage: AttemptStorage
    ) -> ExecutorAliveWatcher:
        return ExecutorAliveWatcher(storage, client_mock)

    async def test_no_attempts(
        self, watcher: ExecutorAliveWatcher, storage: AttemptStorage
    ) -> None:
        await watcher.check()
        assert len([it async for it in storage.list()]) == 0

    async def test_no_running_attempts(
        self, watcher: ExecutorAliveWatcher, storage: AttemptStorage
    ) -> None:
        for _ in range(5):
            data = await self.helper.gen_attempt_data(result=TaskStatus.SUCCEEDED)
            await storage.create(data)
        await watcher.check()
        assert all([it.result == TaskStatus.SUCCEEDED async for it in storage.list()])

    async def test_running_attempt_without_executor_unchanged(
        self, watcher: ExecutorAliveWatcher, storage: AttemptStorage
    ) -> None:
        for _ in range(5):
            data = await self.helper.gen_attempt_data(
                result=TaskStatus.RUNNING,
                executor_id=None,
            )
            await storage.create(data)
        await watcher.check()
        assert all([it.result == TaskStatus.RUNNING async for it in storage.list()])

    async def test_running_attempt_running_executor_not_marked(
        self, client_mock: Mock, watcher: ExecutorAliveWatcher, storage: AttemptStorage
    ) -> None:
        client_mock.jobs.status = AsyncMock(
            return_value=make_descr("test", status=JobStatus.RUNNING)
        )

        data = await self.helper.gen_attempt_data(
            result=TaskStatus.RUNNING,
            executor_id="test",
        )
        attempt = await storage.create(data)
        await watcher.check()
        attempt = await storage.get(attempt.id)
        assert attempt.result == TaskStatus.RUNNING

    async def test_running_attempt_dead_executor_marked_as_failed(
        self, client_mock: Mock, watcher: ExecutorAliveWatcher, storage: AttemptStorage
    ) -> None:
        client_mock.jobs.status = AsyncMock(
            return_value=make_descr("test", status=JobStatus.FAILED)
        )

        data = await self.helper.gen_attempt_data(
            result=TaskStatus.RUNNING,
            executor_id="test",
        )
        attempt = await storage.create(data)
        await watcher.check()
        attempt = await storage.get(attempt.id)
        assert attempt.result == TaskStatus.FAILED

    async def test_running_attempt_canceled_executor_marked_as_failed(
        self, client_mock: Mock, watcher: ExecutorAliveWatcher, storage: AttemptStorage
    ) -> None:
        client_mock.jobs.status = AsyncMock(
            return_value=make_descr("test", status=JobStatus.CANCELLED)
        )

        data = await self.helper.gen_attempt_data(
            result=TaskStatus.RUNNING,
            executor_id="test",
        )
        attempt = await storage.create(data)
        await watcher.check()
        attempt = await storage.get(attempt.id)
        assert attempt.result == TaskStatus.CANCELLED

    async def test_race(
        self, client_mock: Mock, watcher: ExecutorAliveWatcher, storage: AttemptStorage
    ) -> None:
        data = await self.helper.gen_attempt_data(
            result=TaskStatus.RUNNING,
            executor_id="test",
        )
        attempt = await storage.create(data)

        async def do_race(job_id: str) -> JobDescription:
            await storage.update(replace(attempt, result=TaskStatus.SUCCEEDED))
            return make_descr("test", status=JobStatus.SUCCEEDED)

        client_mock.jobs.status = do_race

        await watcher.check()
        attempt = await storage.get(attempt.id)
        assert attempt.result == TaskStatus.SUCCEEDED
