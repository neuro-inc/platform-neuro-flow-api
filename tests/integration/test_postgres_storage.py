from dataclasses import replace
from typing import Optional

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from platform_neuro_flow_api.storage.base import (
    Attempt,
    AttemptStorage,
    BakeImage,
    BakeImageStorage,
    BakeStorage,
    CacheEntryStorage,
    ConfigFileStorage,
    ExistsError,
    LiveJobStorage,
    NotExistsError,
    ProjectStorage,
    TaskStatus,
    TaskStorage,
    UniquenessError,
)
from platform_neuro_flow_api.storage.postgres import (
    PostgresBakeImageStorage,
    PostgresStorage,
    _full_id2str,
)

from tests.unit.test_in_memory_storage import (
    MockDataHelper,
    TestAttemptStorage as _TestAttemptStorage,
    TestBakeImageStorage as _TestBakeImageStorage,
    TestBakeStorage as _TestBakeStorage,
    TestCacheEntryStorage as _TestCacheEntryStorage,
    TestConfigFileStorage as _TestConfigFileStorage,
    TestLiveJobStorage as _TestLiveJobStorage,
    TestProjectStorage as _TestProjectStorage,
    TestTaskStorage as _TestTaskStorage,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
def postgres_storage(sqalchemy_engine: AsyncEngine) -> PostgresStorage:
    return PostgresStorage(sqalchemy_engine)


class TestPostgresProjectStorage(_TestProjectStorage):
    @pytest.fixture(autouse=True)
    def _helper(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> None:
        self.helper = MockDataHelper(postgres_storage)

    @pytest.fixture
    def storage(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> ProjectStorage:
        return postgres_storage.projects


class TestPostgresLiveJobsStorage(_TestLiveJobStorage):
    @pytest.fixture(autouse=True)
    def _helper(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> None:
        self.helper = MockDataHelper(postgres_storage)

    @pytest.fixture
    def storage(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> LiveJobStorage:
        return postgres_storage.live_jobs

    async def test_removed_if_project_deleted(
        self, postgres_storage: PostgresStorage
    ) -> None:
        live_job = await postgres_storage.live_jobs.create(
            await self.helper.gen_live_job_data()
        )
        await postgres_storage.projects.delete(live_job.project_id)
        with pytest.raises(NotExistsError):
            await postgres_storage.live_jobs.get(live_job.id)


class TestPostgresBakeStorage(_TestBakeStorage):
    @pytest.fixture(autouse=True)
    def _helper(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> None:
        self.helper = MockDataHelper(postgres_storage)

    @pytest.fixture
    def storage(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> BakeStorage:
        return postgres_storage.bakes

    async def test_get_by_name_simple(self, storage: BakeStorage) -> None:
        data = await self.helper.gen_bake_data(name="test")
        result_create = await storage.create(data)
        result_get = await storage.get_by_name(data.project_id, "test")
        assert result_create == result_get

    async def test_get_by_name_all_finished(
        self, postgres_storage: PostgresStorage
    ) -> None:
        data = await self.helper.gen_bake_data(name="test")
        bake1 = await postgres_storage.bakes.create(data)
        attempt = await self.helper.gen_attempt_data(
            bake_id=bake1.id, result=TaskStatus.SUCCEEDED
        )
        await postgres_storage.attempts.create(attempt)
        data2 = await self.helper.gen_bake_data(project_id=data.project_id, name="test")
        bake2 = await postgres_storage.bakes.create(data2)
        attempt = await self.helper.gen_attempt_data(
            bake_id=bake2.id, result=TaskStatus.SUCCEEDED
        )
        await postgres_storage.attempts.create(attempt)
        result_get = await postgres_storage.bakes.get_by_name(data.project_id, "test")
        assert bake2 == result_get

    async def test_get_by_name_multiple_attempts(
        self, postgres_storage: PostgresStorage
    ) -> None:
        data = await self.helper.gen_bake_data(name="test")
        bake1 = await postgres_storage.bakes.create(data)
        attempt = await self.helper.gen_attempt_data(
            bake_id=bake1.id, result=TaskStatus.SUCCEEDED
        )
        await postgres_storage.attempts.create(attempt)
        data2 = await self.helper.gen_bake_data(project_id=data.project_id, name="test")
        bake2 = await postgres_storage.bakes.create(data2)
        result_get = await postgres_storage.bakes.get_by_name(data.project_id, "test")
        assert bake2 == result_get

    async def test_no_duplicate_names_if_no_attempts(
        self, storage: BakeStorage
    ) -> None:
        data = await self.helper.gen_bake_data(name="test")
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_duplicate_names_if_status_completed(
        self, postgres_storage: PostgresStorage
    ) -> None:
        data = await self.helper.gen_bake_data(name="test")
        bake1 = await postgres_storage.bakes.create(data)
        attempt = await self.helper.gen_attempt_data(
            bake_id=bake1.id, result=TaskStatus.SUCCEEDED
        )
        await postgres_storage.attempts.create(attempt)
        await postgres_storage.bakes.create(data)

    async def test_cannot_add_attempt_if_name_conflict(
        self, postgres_storage: PostgresStorage
    ) -> None:
        data = await self.helper.gen_bake_data(name="test")
        bake1 = await postgres_storage.bakes.create(data)
        attempt_data = await self.helper.gen_attempt_data(
            number=1, bake_id=bake1.id, result=TaskStatus.SUCCEEDED
        )
        attempt = await postgres_storage.attempts.create(attempt_data)
        await postgres_storage.bakes.create(data)

        attempt = replace(attempt, result=TaskStatus.PENDING)
        attempt_data_2 = replace(attempt_data, number=2, result=TaskStatus.PENDING)
        with pytest.raises(UniquenessError):
            await postgres_storage.attempts.create(attempt_data_2)
        with pytest.raises(UniquenessError):
            await postgres_storage.attempts.update(attempt)

    async def test_removed_if_project_deleted(
        self, postgres_storage: PostgresStorage
    ) -> None:
        bake = await postgres_storage.bakes.create(await self.helper.gen_bake_data())
        await postgres_storage.projects.delete(bake.project_id)
        with pytest.raises(NotExistsError):
            await postgres_storage.bakes.get(bake.id)

    async def test_fetch_last_attempt(self, postgres_storage: PostgresStorage) -> None:
        data1 = await self.helper.gen_bake_data(name="test1")
        data2 = await self.helper.gen_bake_data(name="test2")
        bake1 = await postgres_storage.bakes.create(data1)
        bake2 = await postgres_storage.bakes.create(data2)

        attempt1: Optional[Attempt] = None
        for number in range(5):
            attempt_data = await self.helper.gen_attempt_data(
                number=number, bake_id=bake1.id, result=TaskStatus.SUCCEEDED
            )
            attempt1 = await postgres_storage.attempts.create(attempt_data)
        attempt2: Optional[Attempt] = None
        for number in range(5):
            attempt_data = await self.helper.gen_attempt_data(
                number=number, bake_id=bake2.id, result=TaskStatus.SUCCEEDED
            )
            attempt2 = await postgres_storage.attempts.create(attempt_data)

        assert (
            len(
                [
                    bake
                    async for bake in postgres_storage.bakes.list(
                        name=bake1.name, fetch_last_attempt=True
                    )
                ]
            )
            == 1
        )

        async for bake in postgres_storage.bakes.list(fetch_last_attempt=True):
            if bake.id == bake1.id:
                assert bake.last_attempt == attempt1
            if bake.id == bake2.id:
                assert bake.last_attempt == attempt2

        bake_get = await postgres_storage.bakes.get(
            id=bake1.id, fetch_last_attempt=True
        )
        assert bake_get.last_attempt == attempt1

        bake_get = await postgres_storage.bakes.get(
            id=bake2.id, fetch_last_attempt=True
        )
        assert bake_get.last_attempt == attempt2

        bake_get = await postgres_storage.bakes.get_by_name(
            project_id=bake1.project_id, name="test1", fetch_last_attempt=True
        )
        assert bake_get.last_attempt == attempt1


class TestPostgresAttemptStorage(_TestAttemptStorage):
    @pytest.fixture(autouse=True)
    def _helper(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> None:
        self.helper = MockDataHelper(postgres_storage)

    @pytest.fixture
    def storage(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> AttemptStorage:
        return postgres_storage.attempts

    async def test_removed_if_project_deleted(
        self, postgres_storage: PostgresStorage
    ) -> None:
        attempt = await postgres_storage.attempts.create(
            await self.helper.gen_attempt_data()
        )
        bake = await postgres_storage.bakes.get(attempt.bake_id)
        await postgres_storage.projects.delete(bake.project_id)
        with pytest.raises(NotExistsError):
            await postgres_storage.attempts.get(attempt.id)


class TestPostgresTaskStorage(_TestTaskStorage):
    @pytest.fixture(autouse=True)
    def _helper(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> None:
        self.helper = MockDataHelper(postgres_storage)

    @pytest.fixture
    def storage(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> TaskStorage:
        return postgres_storage.tasks

    async def test_removed_if_project_deleted(
        self, postgres_storage: PostgresStorage
    ) -> None:
        task = await postgres_storage.tasks.create(await self.helper.gen_task_data())
        attempt = await postgres_storage.attempts.get(task.attempt_id)
        bake = await postgres_storage.bakes.get(attempt.bake_id)
        await postgres_storage.projects.delete(bake.project_id)
        with pytest.raises(NotExistsError):
            await postgres_storage.tasks.get(task.id)


class TestPostgresCacheEntryStorage(_TestCacheEntryStorage):
    @pytest.fixture(autouse=True)
    def _helper(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> None:
        self.helper = MockDataHelper(postgres_storage)

    @pytest.fixture
    def storage(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> CacheEntryStorage:
        return postgres_storage.cache_entries

    async def test_removed_if_project_deleted(
        self, postgres_storage: PostgresStorage
    ) -> None:
        entry = await postgres_storage.cache_entries.create(
            await self.helper.gen_cache_entry_data()
        )
        await postgres_storage.projects.delete(entry.project_id)
        with pytest.raises(NotExistsError):
            await postgres_storage.cache_entries.get(entry.id)


class TestPostgresConfigFileStorage(_TestConfigFileStorage):
    @pytest.fixture(autouse=True)
    def _helper(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> None:
        self.helper = MockDataHelper(postgres_storage)

    @pytest.fixture
    def storage(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> ConfigFileStorage:
        return postgres_storage.config_files

    async def test_removed_if_project_deleted(
        self, postgres_storage: PostgresStorage
    ) -> None:
        file = await postgres_storage.config_files.create(
            await self.helper.gen_config_file_data()
        )
        bake = await postgres_storage.bakes.get(file.bake_id)
        await postgres_storage.projects.delete(bake.project_id)
        with pytest.raises(NotExistsError):
            await postgres_storage.config_files.get(file.id)


class TestPostgresBakeImageStorage(_TestBakeImageStorage):
    @pytest.fixture(autouse=True)
    def _helper(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> None:
        self.helper = MockDataHelper(postgres_storage)

    @pytest.fixture
    def storage(  # type: ignore
        self,
        postgres_storage: PostgresStorage,
    ) -> BakeImageStorage:
        return postgres_storage.bake_images

    async def test_read_old_entry(
        self,
        storage: BakeImageStorage,
    ) -> None:
        assert isinstance(storage, PostgresBakeImageStorage)
        data = await self.helper.gen_bake_image_data()
        image = BakeImage.from_data_obj("test-id", data)
        values = storage._to_values(image)
        values["payload"].pop("yaml_defs")
        values["payload"]["prefix"] = _full_id2str(image.prefix)
        values["payload"]["yaml_id"] = image.yaml_id
        query = storage._table.insert().values(values)
        async with storage._engine.begin() as conn:
            await storage._execute(query, conn)

        image_from_db = await storage.get("test-id")
        assert image == image_from_db

    async def test_removed_if_project_deleted(
        self, postgres_storage: PostgresStorage
    ) -> None:
        image = await postgres_storage.bake_images.create(
            await self.helper.gen_bake_image_data()
        )
        bake = await postgres_storage.bakes.get(image.bake_id)
        await postgres_storage.projects.delete(bake.project_id)
        with pytest.raises(NotExistsError):
            await postgres_storage.bake_images.get(image.id)
