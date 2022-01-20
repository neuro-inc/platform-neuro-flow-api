from __future__ import annotations

import secrets
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from platform_neuro_flow_api.storage.base import (
    Attempt,
    AttemptData,
    AttemptStorage,
    Bake,
    BakeData,
    BakeImage,
    BakeImageData,
    BakeImageStorage,
    BakeStorage,
    CacheEntry,
    CacheEntryData,
    CacheEntryStorage,
    ConfigFile,
    ConfigFileData,
    ConfigFileStorage,
    ConfigsMeta,
    ExistsError,
    ImageStatus,
    LiveJob,
    LiveJobData,
    LiveJobStorage,
    NotExistsError,
    Project,
    ProjectData,
    ProjectStorage,
    Storage,
    Task,
    TaskData,
    TaskStatus,
    TaskStatusItem,
    TaskStorage,
)
from platform_neuro_flow_api.storage.in_memory import InMemoryStorage


class MockDataHelper:
    def __init__(self, storage: Storage):
        self._storage = storage

    async def _ensure_project_id(self, project_id: str) -> None:
        data = await self.gen_project_data()
        project = Project.from_data_obj(project_id, data)
        try:
            await self._storage.projects.insert(project)
        except ExistsError:
            pass

    async def _ensure_bake_id(self, bake_id: str) -> None:
        data = await self.gen_bake_data()
        item = Bake.from_data_obj(bake_id, data)
        try:
            await self._storage.bakes.insert(item)
        except ExistsError:
            pass

    async def _ensure_attempt_id(self, attempt_id: str) -> None:
        data = await self.gen_attempt_data()
        item = Attempt.from_data_obj(attempt_id, data)
        try:
            await self._storage.attempts.insert(item)
        except ExistsError:
            pass

    async def _ensure_config_file_id(self, config_file_id: str) -> None:
        data = await self.gen_config_file_data()
        item = ConfigFile.from_data_obj(config_file_id, data)
        try:
            await self._storage.config_files.insert(item)
        except ExistsError:
            pass

    async def gen_project_data(self, **kwargs: Any) -> ProjectData:
        data = ProjectData(
            name=secrets.token_hex(8),
            owner=secrets.token_hex(8),
            cluster=secrets.token_hex(8),
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        return data

    async def gen_live_job_data(self, **kwargs: Any) -> LiveJobData:
        data = LiveJobData(
            yaml_id=secrets.token_hex(8),
            project_id=secrets.token_hex(8),
            multi=False,
            tags=[secrets.token_hex(8), secrets.token_hex(8)],
            raw_id=secrets.token_hex(8),
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        await self._ensure_project_id(data.project_id)
        return data

    async def gen_bake_data(self, **kwargs: Any) -> BakeData:
        data = BakeData(
            project_id=secrets.token_hex(8),
            batch=secrets.token_hex(8),
            created_at=datetime.now(timezone.utc),
            graphs={},
            params=None,
            tags=(),
            name=secrets.token_hex(8),
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        await self._ensure_project_id(data.project_id)
        return data

    async def gen_attempt_data(self, **kwargs: Any) -> AttemptData:
        data = AttemptData(
            bake_id=secrets.token_hex(8),
            number=secrets.randbits(20),
            created_at=datetime.now(timezone.utc),
            result=TaskStatus.PENDING,
            executor_id=secrets.token_hex(8),
            configs_meta=ConfigsMeta(
                workspace=secrets.token_hex(8),
                flow_config_id=secrets.token_hex(8),
                project_config_id=secrets.token_hex(8),
                action_config_ids={},
            ),
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        await self._ensure_bake_id(data.bake_id)
        await self._ensure_config_file_id(data.configs_meta.flow_config_id)
        if data.configs_meta.project_config_id is not None:
            await self._ensure_config_file_id(data.configs_meta.project_config_id)
        for config_file_id in data.configs_meta.action_config_ids.values():
            await self._ensure_config_file_id(config_file_id)
        return data

    async def gen_task_data(self, **kwargs: Any) -> TaskData:
        data = TaskData(
            yaml_id=(secrets.token_hex(8),),
            attempt_id=secrets.token_hex(8),
            raw_id=None,
            outputs=None,
            state=None,
            statuses=[
                TaskStatusItem(
                    when=datetime.now(timezone.utc),
                    status=TaskStatus.PENDING,
                )
            ],
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        await self._ensure_attempt_id(data.attempt_id)
        return data

    async def gen_cache_entry_data(self, **kwargs: Any) -> CacheEntryData:
        data = CacheEntryData(
            project_id=secrets.token_hex(8),
            task_id=(secrets.token_hex(8),),
            batch=secrets.token_hex(8),
            key=secrets.token_hex(8),
            outputs={},
            state={},
            created_at=datetime.now(timezone.utc),
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        await self._ensure_project_id(data.project_id)
        return data

    async def gen_config_file_data(self, **kwargs: Any) -> ConfigFileData:
        data = ConfigFileData(
            bake_id=secrets.token_hex(8),
            filename=secrets.token_hex(8),
            content=secrets.token_hex(8),
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        await self._ensure_bake_id(data.bake_id)
        return data

    async def gen_bake_image_data(self, **kwargs: Any) -> BakeImageData:
        data = BakeImageData(
            bake_id=secrets.token_hex(8),
            ref=secrets.token_hex(20),
            yaml_defs=[(secrets.token_hex(8),)],
            context_on_storage=None,
            dockerfile_rel=None,
            status=ImageStatus.PENDING,
            builder_job_id=None,
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        await self._ensure_bake_id(data.bake_id)
        return data


class TestProjectStorage:
    helper: MockDataHelper

    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> ProjectStorage:
        return in_memory_storage.projects

    @pytest.fixture(autouse=True)
    def _helper(self, in_memory_storage: InMemoryStorage) -> None:
        self.helper = MockDataHelper(in_memory_storage)

    def compare_data(self, data1: ProjectData, data2: ProjectData) -> None:
        d1 = asdict(data1)
        d1.pop("id", None)
        d2 = asdict(data2)
        d2.pop("id", None)
        assert d1 == d2

    async def test_create_get_update(self, storage: ProjectStorage) -> None:
        data = await self.helper.gen_project_data()
        created = await storage.create(data)
        self.compare_data(data, created)
        res = await storage.get(created.id)
        self.compare_data(res, created)
        assert res.id == created.id
        updated = replace(res, name="new-name")
        await storage.update(updated)
        project = await storage.get(res.id)
        assert project.name == updated.name

    async def test_get_not_exists(self, storage: ProjectStorage) -> None:
        with pytest.raises(NotExistsError):
            await storage.get("wrong-id")

    async def test_update_not_exists(self, storage: ProjectStorage) -> None:
        with pytest.raises(NotExistsError):
            data = await self.helper.gen_project_data()
            project = Project.from_data_obj("test", data)
            await storage.update(project)

    async def test_get_by_name(self, storage: ProjectStorage) -> None:
        data = await self.helper.gen_project_data()
        res = await storage.create(data)
        project = await storage.get_by_name(
            name=data.name, owner=data.owner, cluster=data.cluster
        )
        assert project.id == res.id

    async def test_get_by_name_wrong_owner(self, storage: ProjectStorage) -> None:
        data = await self.helper.gen_project_data()
        await storage.create(data)
        with pytest.raises(NotExistsError):
            await storage.get_by_name(
                name=data.name, owner="wrong_owner", cluster=data.cluster
            )

    async def test_cannot_create_duplicate(self, storage: ProjectStorage) -> None:
        data = await self.helper.gen_project_data()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_list(self, storage: ProjectStorage) -> None:
        for name_id in range(5):
            for owner_id in range(5):
                for cluster_id in range(5):
                    data = await self.helper.gen_project_data(
                        name=f"name-{name_id}",
                        owner=f"owner-{owner_id}",
                        cluster=f"cluster-{cluster_id}",
                    )
                    await storage.create(data)
        found = []
        async for item in storage.list(name="name-1", owner="owner-2"):
            found.append(item.cluster)
        assert len(found) == 5
        assert set(found) == {f"cluster-{index}" for index in range(5)}

    async def test_delete(self, storage: ProjectStorage) -> None:
        data = await self.helper.gen_project_data()
        project = await storage.create(data)
        await storage.delete(project.id)
        with pytest.raises(NotExistsError):
            await storage.get(project.id)


class TestLiveJobStorage:

    helper: MockDataHelper

    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> LiveJobStorage:
        return in_memory_storage.live_jobs

    @pytest.fixture(autouse=True)
    def _helper(self, in_memory_storage: InMemoryStorage) -> None:
        self.helper = MockDataHelper(in_memory_storage)

    def compare_data(self, data1: LiveJobData, data2: LiveJobData) -> None:
        d1 = asdict(data1)
        d1.pop("id", None)
        d2 = asdict(data2)
        d2.pop("id", None)
        assert d1 == d2

    async def test_create_get_update(self, storage: LiveJobStorage) -> None:
        data = await self.helper.gen_live_job_data()
        created = await storage.create(data)
        self.compare_data(data, created)
        res = await storage.get(created.id)
        self.compare_data(res, created)
        assert res.id == created.id
        updated = replace(res, yaml_id="new-yaml-id")
        await storage.update(updated)
        project = await storage.get(res.id)
        assert project.yaml_id == updated.yaml_id

    async def test_get_not_exists(self, storage: LiveJobStorage) -> None:
        with pytest.raises(NotExistsError):
            await storage.get("wrong-id")

    async def test_update_not_exists(self, storage: LiveJobStorage) -> None:
        with pytest.raises(NotExistsError):
            data = await self.helper.gen_live_job_data()
            item = LiveJob.from_data_obj("test", data)
            await storage.update(item)

    async def test_get_by_yaml_id(self, storage: LiveJobStorage) -> None:
        data = await self.helper.gen_live_job_data()
        res = await storage.create(data)
        project = await storage.get_by_yaml_id(
            yaml_id=data.yaml_id, project_id=data.project_id
        )
        assert project.id == res.id

    async def test_cannot_create_duplicate(self, storage: LiveJobStorage) -> None:
        data = await self.helper.gen_live_job_data()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_list(self, storage: LiveJobStorage) -> None:
        for yaml_id_num in range(5):
            for project_id in range(5):
                data = await self.helper.gen_live_job_data(
                    yaml_id=f"yaml-id-{yaml_id_num}",
                    project_id=f"project-{project_id}",
                )
                await storage.create(data)
        found = []
        async for item in storage.list(project_id="project-2"):
            found.append(item.yaml_id)
        assert len(found) == 5
        assert set(found) == {f"yaml-id-{index}" for index in range(5)}


class TestBakeStorage:

    helper: MockDataHelper

    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> BakeStorage:
        return in_memory_storage.bakes

    @pytest.fixture(autouse=True)
    def _helper(self, in_memory_storage: InMemoryStorage) -> None:
        self.helper = MockDataHelper(in_memory_storage)

    def compare_data(self, data1: BakeData, data2: BakeData) -> None:
        d1 = asdict(data1)
        d1.pop("id", None)
        d2 = asdict(data2)
        d2.pop("id", None)
        assert d1 == d2

    async def test_create_get_update(self, storage: BakeStorage) -> None:
        data = await self.helper.gen_bake_data()
        created = await storage.create(data)
        self.compare_data(data, created)
        res = await storage.get(created.id)
        self.compare_data(res, created)
        assert res.id == created.id
        updated = replace(res, batch="another_batch", tags=("foo", "bar"))
        await storage.update(updated)
        bake = await storage.get(res.id)
        assert bake.batch == updated.batch
        assert bake.tags == updated.tags

    async def test_get_not_exists(self, storage: BakeStorage) -> None:
        with pytest.raises(NotExistsError):
            await storage.get("wrong-id")

    async def test_update_not_exists(self, storage: BakeStorage) -> None:
        with pytest.raises(NotExistsError):
            data = await self.helper.gen_bake_data()
            item = Bake.from_data_obj("test", data)
            await storage.update(item)

    async def test_list(self, storage: BakeStorage) -> None:
        for batch_id in range(5):
            for project_id in range(5):
                data = await self.helper.gen_bake_data(
                    batch=f"batch-id-{batch_id}",
                    project_id=f"project-{project_id}",
                )
                await storage.create(data)
        found = []
        async for item in storage.list(project_id="project-2"):
            found.append(item.batch)
        assert len(found) == 5
        assert set(found) == {f"batch-id-{index}" for index in range(5)}

    async def test_list_by_tags(self, storage: BakeStorage) -> None:
        for tag in range(5):
            data = await self.helper.gen_bake_data(tags=(f"tag-{tag}",))
            await storage.create(data)

        data = await self.helper.gen_bake_data(tags=("tag-1", "tag-2"))
        await storage.create(data)

        found = []
        async for item in storage.list(tags={"tag-1"}):
            found.append(item.batch)
        assert len(found) == 2

        found = []
        async for item in storage.list(tags={"tag-2"}):
            found.append(item.batch)
        assert len(found) == 2

        found = []
        async for item in storage.list(tags={"tag-3"}):
            found.append(item.batch)
        assert len(found) == 1

        found = []
        async for item in storage.list(tags={"tag-1", "tag-2"}):
            found.append(item.batch)
        assert len(found) == 1

    async def test_list_by_name(self, storage: BakeStorage) -> None:
        for name_id in range(5):
            data = await self.helper.gen_bake_data(name=f"name-{name_id}")
            await storage.create(data)

        data = await self.helper.gen_bake_data(name="name-1")
        await storage.create(data)

        found = []
        async for item in storage.list(name="name-1"):
            found.append(item.batch)
        assert len(found) == 2

        found = []
        async for item in storage.list(name="name-3"):
            found.append(item.batch)
        assert len(found) == 1

        found = []
        async for item in storage.list(name="unknown"):
            found.append(item.batch)
        assert len(found) == 0

    async def test_list_order(self, storage: BakeStorage) -> None:
        now = datetime.now()
        for batch_id in range(5):
            for project_id in range(5):
                data = await self.helper.gen_bake_data(
                    batch=f"batch-id-{batch_id}",
                    project_id=f"project-{project_id}",
                    created_at=now + timedelta(seconds=batch_id),
                )
                await storage.create(data)
        found = []
        async for item in storage.list(project_id="project-2", reverse=False):
            found.append(item.batch)
        assert found == [f"batch-id-{index}" for index in range(0, 5)]

        found = []
        async for item in storage.list(project_id="project-2", reverse=True):
            found.append(item.batch)
        assert found == [f"batch-id-{index}" for index in range(4, -1, -1)]

    async def test_list_since(self, storage: BakeStorage) -> None:
        now = datetime.now()
        for batch_id in range(5):
            for project_id in range(5):
                data = await self.helper.gen_bake_data(
                    batch=f"batch-id-{batch_id}",
                    project_id=f"project-{project_id}",
                    created_at=now + timedelta(seconds=batch_id),
                )
                await storage.create(data)
        found = []
        async for item in storage.list(
            project_id="project-2", since=now + timedelta(seconds=2.5)
        ):
            found.append(item.batch)
        assert found == [f"batch-id-{index}" for index in range(3, 5)]

    async def test_list_until(self, storage: BakeStorage) -> None:
        now = datetime.now()
        for batch_id in range(5):
            for project_id in range(5):
                data = await self.helper.gen_bake_data(
                    batch=f"batch-id-{batch_id}",
                    project_id=f"project-{project_id}",
                    created_at=now + timedelta(seconds=batch_id),
                )
                await storage.create(data)
        found = []
        async for item in storage.list(
            project_id="project-2", until=now + timedelta(seconds=2.5)
        ):
            found.append(item.batch)
        assert found == [f"batch-id-{index}" for index in range(0, 3)]


class TestAttemptStorage:
    helper: MockDataHelper

    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> AttemptStorage:
        return in_memory_storage.attempts

    @pytest.fixture(autouse=True)
    def _helper(self, in_memory_storage: InMemoryStorage) -> None:
        self.helper = MockDataHelper(in_memory_storage)

    def compare_data(self, data1: AttemptData, data2: AttemptData) -> None:
        d1 = asdict(data1)
        d1.pop("id", None)
        d2 = asdict(data2)
        d2.pop("id", None)
        assert d1 == d2

    async def test_create_get_update(self, storage: AttemptStorage) -> None:
        data = await self.helper.gen_attempt_data()
        created = await storage.create(data)
        self.compare_data(data, created)
        res = await storage.get(created.id)
        self.compare_data(res, created)
        assert res.id == created.id
        updated = replace(res, number=42)
        await storage.update(updated)
        project = await storage.get(res.id)
        assert project.bake_id == updated.bake_id

    async def test_get_not_exists(self, storage: AttemptStorage) -> None:
        with pytest.raises(NotExistsError):
            await storage.get("wrong-id")

    async def test_update_not_exists(self, storage: AttemptStorage) -> None:
        with pytest.raises(NotExistsError):
            data = await self.helper.gen_attempt_data()
            item = Attempt.from_data_obj("test", data)
            await storage.update(item)

    async def test_get_by_number(self, storage: AttemptStorage) -> None:
        data = await self.helper.gen_attempt_data()
        res = await storage.create(data)
        project = await storage.get_by_number(
            bake_id=data.bake_id,
            number=data.number,
        )
        assert project.id == res.id

    async def test_cannot_create_duplicate(self, storage: AttemptStorage) -> None:
        data = await self.helper.gen_attempt_data()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_list(self, storage: AttemptStorage) -> None:
        for bake_id in range(5):
            for number in range(5):
                data = await self.helper.gen_attempt_data(
                    bake_id=f"bake-id-{bake_id}",
                    number=number,
                )
                await storage.create(data)
        found = []
        async for item in storage.list(bake_id="bake-id-2"):
            found.append(item.number)
        assert len(found) == 5
        assert set(found) == set(range(5))


class TestTaskStorage:
    helper: MockDataHelper

    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> TaskStorage:
        return in_memory_storage.tasks

    @pytest.fixture(autouse=True)
    def _helper(self, in_memory_storage: InMemoryStorage) -> None:
        self.helper = MockDataHelper(in_memory_storage)

    def compare_data(self, data1: TaskData, data2: TaskData) -> None:
        d1 = asdict(data1)
        d1.pop("id", None)
        d2 = asdict(data2)
        d2.pop("id", None)
        assert d1 == d2

    async def test_create_get_update(self, storage: TaskStorage) -> None:
        data = await self.helper.gen_task_data()
        created = await storage.create(data)
        self.compare_data(data, created)
        res = await storage.get(created.id)
        self.compare_data(res, created)
        assert res.id == created.id
        updated = replace(res, yaml_id=("new-yaml-id",))
        await storage.update(updated)
        project = await storage.get(res.id)
        assert project.yaml_id == updated.yaml_id

    async def test_get_not_exists(self, storage: TaskStorage) -> None:
        with pytest.raises(NotExistsError):
            await storage.get("wrong-id")

    async def test_update_not_exists(self, storage: TaskStorage) -> None:
        with pytest.raises(NotExistsError):
            data = await self.helper.gen_task_data()
            item = Task.from_data_obj("test", data)
            await storage.update(item)

    async def test_get_by_yaml_id(self, storage: TaskStorage) -> None:
        data = await self.helper.gen_task_data()
        res = await storage.create(data)
        project = await storage.get_by_yaml_id(
            attempt_id=data.attempt_id,
            yaml_id=data.yaml_id,
        )
        assert project.id == res.id

    async def test_cannot_create_duplicate(self, storage: TaskStorage) -> None:
        data = await self.helper.gen_task_data()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_list(self, storage: TaskStorage) -> None:
        for attempt_id in range(5):
            for raw_id in range(5):
                data = await self.helper.gen_task_data(
                    attempt_id=f"attempt-id-{attempt_id}",
                    raw_id=f"raw-id-{raw_id}",
                )
                await storage.create(data)
        found = []
        async for item in storage.list(attempt_id="attempt-id-2"):
            found.append(item.raw_id)
        assert len(found) == 5
        assert set(found) == {f"raw-id-{index}" for index in range(5)}


class TestCacheEntryStorage:
    helper: MockDataHelper

    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> CacheEntryStorage:
        return in_memory_storage.cache_entries

    @pytest.fixture(autouse=True)
    def _helper(self, in_memory_storage: InMemoryStorage) -> None:
        self.helper = MockDataHelper(in_memory_storage)

    def compare_data(self, data1: CacheEntryData, data2: CacheEntryData) -> None:
        d1 = asdict(data1)
        d1.pop("id", None)
        d2 = asdict(data2)
        d2.pop("id", None)
        assert d1 == d2

    async def test_create_get_update(self, storage: CacheEntryStorage) -> None:
        data = await self.helper.gen_cache_entry_data()
        created = await storage.create(data)
        self.compare_data(data, created)
        res = await storage.get(created.id)
        self.compare_data(res, created)
        assert res.id == created.id
        updated = replace(res, task_id=("new-yaml-id",))
        await storage.update(updated)
        project = await storage.get(res.id)
        assert project.task_id == updated.task_id

    async def test_get_not_exists(self, storage: CacheEntryStorage) -> None:
        with pytest.raises(NotExistsError):
            await storage.get("wrong-id")

    async def test_update_not_exists(self, storage: CacheEntryStorage) -> None:
        with pytest.raises(NotExistsError):
            data = await self.helper.gen_cache_entry_data()
            item = CacheEntry.from_data_obj("test", data)
            await storage.update(item)

    async def test_get_by_yaml_id(self, storage: CacheEntryStorage) -> None:
        data = await self.helper.gen_cache_entry_data()
        res = await storage.create(data)
        item = await storage.get_by_key(
            task_id=data.task_id,
            project_id=data.project_id,
            batch=data.batch,
            key=data.key,
        )
        assert item.id == res.id

    async def test_cannot_create_duplicate(self, storage: CacheEntryStorage) -> None:
        data = await self.helper.gen_cache_entry_data()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_delete_all(self, storage: CacheEntryStorage) -> None:
        for key_id in range(5):
            for batch_id in range(5):
                for task_id in range(5):
                    for project_id in range(5):
                        data = await self.helper.gen_cache_entry_data(
                            project_id=f"project-id-{project_id}",
                            key=f"key-{key_id}",
                            batch=f"batch-{batch_id}",
                            task_id=(f"task-{task_id}",),
                        )
                        await storage.create(data)
        await storage.get_by_key(
            project_id="project-id-2", key="key-2", batch="batch-2", task_id=("task-2",)
        )

        await storage.delete_all(project_id="project-id-2")

        await storage.get_by_key(
            project_id="project-id-1", key="key-2", batch="batch-2", task_id=("task-2",)
        )
        with pytest.raises(NotExistsError):
            await storage.get_by_key(
                project_id="project-id-2",
                key="key-2",
                batch="batch-2",
                task_id=("task-2",),
            )


class TestConfigFileStorage:
    helper: MockDataHelper

    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> ConfigFileStorage:
        return in_memory_storage.config_files

    @pytest.fixture(autouse=True)
    def _helper(self, in_memory_storage: InMemoryStorage) -> None:
        self.helper = MockDataHelper(in_memory_storage)

    def compare_data(self, data1: ConfigFileData, data2: ConfigFileData) -> None:
        d1 = asdict(data1)
        d1.pop("id", None)
        d2 = asdict(data2)
        d2.pop("id", None)
        assert d1 == d2

    async def test_create_get_update(self, storage: ConfigFileStorage) -> None:
        data = await self.helper.gen_config_file_data()
        created = await storage.create(data)
        self.compare_data(data, created)
        res = await storage.get(created.id)
        self.compare_data(res, created)
        assert res.id == created.id
        updated = replace(res, content="new-content")
        await storage.update(updated)
        project = await storage.get(res.id)
        assert project.content == updated.content

    async def test_get_not_exists(self, storage: ConfigFileStorage) -> None:
        with pytest.raises(NotExistsError):
            await storage.get("wrong-id")

    async def test_update_not_exists(self, storage: ConfigFileStorage) -> None:
        with pytest.raises(NotExistsError):
            data = await self.helper.gen_config_file_data()
            item = ConfigFile.from_data_obj("test", data)
            await storage.update(item)


class TestBakeImageStorage:
    helper: MockDataHelper

    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> BakeImageStorage:
        return in_memory_storage.bake_images

    @pytest.fixture(autouse=True)
    def _helper(self, in_memory_storage: InMemoryStorage) -> None:
        self.helper = MockDataHelper(in_memory_storage)

    def compare_data(self, data1: BakeImageData, data2: BakeImageData) -> None:
        d1 = asdict(data1)
        d1.pop("id", None)
        d2 = asdict(data2)
        d2.pop("id", None)
        assert d1 == d2

    async def test_create_get_update(self, storage: BakeImageStorage) -> None:
        data = await self.helper.gen_bake_image_data()
        created = await storage.create(data)
        self.compare_data(data, created)
        res = await storage.get(created.id)
        self.compare_data(res, created)
        assert res.id == created.id
        updated = replace(res, ref="foo")
        await storage.update(updated)
        project = await storage.get(res.id)
        assert project.bake_id == updated.bake_id

    async def test_get_not_exists(self, storage: BakeImageStorage) -> None:
        with pytest.raises(NotExistsError):
            await storage.get("wrong-id")

    async def test_update_not_exists(self, storage: BakeImageStorage) -> None:
        with pytest.raises(NotExistsError):
            data = await self.helper.gen_bake_image_data()
            item = BakeImage.from_data_obj("test", data)
            await storage.update(item)

    async def test_get_by_ref(self, storage: BakeImageStorage) -> None:
        data = await self.helper.gen_bake_image_data()
        res = await storage.create(data)
        result = await storage.get_by_ref(
            bake_id=data.bake_id,
            ref=data.ref,
        )
        assert result.id == res.id

    async def test_cannot_create_duplicate(self, storage: BakeImageStorage) -> None:
        data = await self.helper.gen_bake_image_data()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_list(self, storage: BakeImageStorage) -> None:
        for bake_id in range(5):
            for number in range(5):
                data = await self.helper.gen_bake_image_data(
                    bake_id=f"bake-id-{bake_id}",
                    ref=str(number),
                )
                await storage.create(data)
        found = []
        async for item in storage.list(bake_id="bake-id-2"):
            found.append(item.ref)
        assert len(found) == 5
        assert set(found) == {str(index) for index in range(5)}
