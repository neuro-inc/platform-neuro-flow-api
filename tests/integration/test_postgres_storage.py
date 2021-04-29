from dataclasses import replace

import pytest
from asyncpg import Pool

from platform_neuro_flow_api.storage.base import (
    AttemptStorage,
    BakeStorage,
    CacheEntryStorage,
    ConfigFileStorage,
    ExistsError,
    LiveJobStorage,
    ProjectStorage,
    TaskStatus,
    TaskStorage,
    UniquenessError,
)
from platform_neuro_flow_api.storage.postgres import PostgresStorage
from tests.unit.test_in_memory_storage import (
    MockDataHelper,
    TestAttemptStorage as _TestAttemptStorage,
    TestBakeStorage as _TestBakeStorage,
    TestCacheEntryStorage as _TestCacheEntryStorage,
    TestConfigFileStorage as _TestConfigFileStorage,
    TestLiveJobStorage as _TestLiveJobStorage,
    TestProjectStorage as _TestProjectStorage,
    TestTaskStorage as _TestTaskStorage,
)


pytestmark = pytest.mark.asyncio


@pytest.fixture
def postgres_storage(postgres_pool: Pool) -> PostgresStorage:
    return PostgresStorage(postgres_pool)


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

    async def test_get_by_name_multiple_attempts(
        self, postgres_storage: PostgresStorage
    ) -> None:
        data = await self.helper.gen_bake_data(name="test")
        bake1 = await postgres_storage.bakes.create(data)
        attempt = await self.helper.gen_attempt_data(
            bake_id=bake1.id, result=TaskStatus.SUCCEEDED
        )
        await postgres_storage.attempts.create(attempt)
        bake2 = await postgres_storage.bakes.create(data)
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
