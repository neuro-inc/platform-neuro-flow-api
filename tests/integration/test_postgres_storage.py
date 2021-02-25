import pytest
from asyncpg import Pool

from platform_neuro_flow_api.storage.base import (
    AttemptStorage,
    BakeStorage,
    CacheEntryStorage,
    ConfigFileStorage,
    LiveJobStorage,
    ProjectStorage,
    TaskStorage,
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
