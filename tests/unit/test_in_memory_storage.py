import secrets
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

import pytest

from platform_neuro_flow_api.storage.base import (
    Attempt,
    AttemptData,
    AttemptStorage,
    Bake,
    BakeData,
    BakeStorage,
    CacheEntry,
    CacheEntryData,
    CacheEntryStorage,
    ConfigFile,
    ConfigFileData,
    ConfigFileStorage,
    ConfigsMeta,
    ExistsError,
    LiveJob,
    LiveJobData,
    LiveJobStorage,
    NotExistsError,
    Project,
    ProjectData,
    ProjectStorage,
    Task,
    TaskData,
    TaskStatus,
    TaskStatusItem,
    TaskStorage,
)
from platform_neuro_flow_api.storage.in_memory import InMemoryStorage


pytestmark = pytest.mark.asyncio


class TestProjectStorage:
    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> ProjectStorage:
        return in_memory_storage.projects

    def data_factory(self, **kwargs: Any) -> ProjectData:
        data = ProjectData(
            name=secrets.token_hex(8),
            owner=secrets.token_hex(8),
            cluster=secrets.token_hex(8),
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        return data

    def item_factory(self, **kwargs: Any) -> Project:
        data = self.data_factory()
        item = Project.from_data_obj(secrets.token_hex(8), data)
        for key, value in kwargs.items():
            item = replace(item, **{key: value})
        return item

    def compare_data(self, data1: ProjectData, data2: ProjectData) -> bool:
        return ProjectData.__eq__(data1, data2)

    async def test_create_get_update(self, storage: ProjectStorage) -> None:
        data = self.data_factory()
        created = await storage.create(data)
        assert self.compare_data(data, created)
        res = await storage.get(created.id)
        assert self.compare_data(res, created)
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
            project = self.item_factory()
            await storage.update(project)

    async def test_get_by_name(self, storage: ProjectStorage) -> None:
        data = self.data_factory()
        res = await storage.create(data)
        project = await storage.get_by_name(
            name=data.name, owner=data.owner, cluster=data.cluster
        )
        assert project.id == res.id

    async def test_cannot_create_duplicate(self, storage: ProjectStorage) -> None:
        data = self.data_factory()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_list(self, storage: ProjectStorage) -> None:
        for name_id in range(5):
            for owner_id in range(5):
                for cluster_id in range(5):
                    data = self.data_factory(
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


class TestLiveJobStorage:
    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> LiveJobStorage:
        return in_memory_storage.live_jobs

    def data_factory(self, **kwargs: Any) -> LiveJobData:
        data = LiveJobData(
            yaml_id=secrets.token_hex(8),
            project_id=secrets.token_hex(8),
            multi=False,
            tags=[secrets.token_hex(8), secrets.token_hex(8)],
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        return data

    def item_factory(self, **kwargs: Any) -> LiveJob:
        data = self.data_factory()
        item = LiveJob.from_data_obj(secrets.token_hex(8), data)
        for key, value in kwargs.items():
            item = replace(item, **{key: value})
        return item

    def compare_data(self, data1: LiveJobData, data2: LiveJobData) -> bool:
        return LiveJobData.__eq__(data1, data2)

    async def test_create_get_update(self, storage: LiveJobStorage) -> None:
        data = self.data_factory()
        created = await storage.create(data)
        assert self.compare_data(data, created)
        res = await storage.get(created.id)
        assert self.compare_data(res, created)
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
            item = self.item_factory()
            await storage.update(item)

    async def test_get_by_yaml_id(self, storage: LiveJobStorage) -> None:
        data = self.data_factory()
        res = await storage.create(data)
        project = await storage.get_by_yaml_id(
            yaml_id=data.yaml_id, project_id=data.project_id
        )
        assert project.id == res.id

    async def test_cannot_create_duplicate(self, storage: LiveJobStorage) -> None:
        data = self.data_factory()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_list(self, storage: LiveJobStorage) -> None:
        for yaml_id_num in range(5):
            for project_id in range(5):
                data = self.data_factory(
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
    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> BakeStorage:
        return in_memory_storage.bakes

    def data_factory(self, **kwargs: Any) -> BakeData:
        data = BakeData(
            project_id=secrets.token_hex(8),
            batch=secrets.token_hex(8),
            created_at=datetime.now(timezone.utc),
            graphs={},
            params=None,
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        return data

    def item_factory(self, **kwargs: Any) -> Bake:
        data = self.data_factory()
        item = Bake.from_data_obj(secrets.token_hex(8), data)
        for key, value in kwargs.items():
            item = replace(item, **{key: value})
        return item

    def compare_data(self, data1: BakeData, data2: BakeData) -> bool:
        return BakeData.__eq__(data1, data2)

    async def test_create_get_update(self, storage: BakeStorage) -> None:
        data = self.data_factory()
        created = await storage.create(data)
        assert self.compare_data(data, created)
        res = await storage.get(created.id)
        assert self.compare_data(res, created)
        assert res.id == created.id
        updated = replace(res, batch="another_batch")
        await storage.update(updated)
        project = await storage.get(res.id)
        assert project.batch == updated.batch

    async def test_get_not_exists(self, storage: BakeStorage) -> None:
        with pytest.raises(NotExistsError):
            await storage.get("wrong-id")

    async def test_update_not_exists(self, storage: BakeStorage) -> None:
        with pytest.raises(NotExistsError):
            item = self.item_factory()
            await storage.update(item)

    async def test_list(self, storage: BakeStorage) -> None:
        for batch_id in range(5):
            for project_id in range(5):
                data = self.data_factory(
                    batch=f"batch-id-{batch_id}",
                    project_id=f"project-{project_id}",
                )
                await storage.create(data)
        found = []
        async for item in storage.list(project_id="project-2"):
            found.append(item.batch)
        assert len(found) == 5
        assert set(found) == {f"batch-id-{index}" for index in range(5)}


class TestAttemptStorage:
    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> AttemptStorage:
        return in_memory_storage.attempts

    def data_factory(self, **kwargs: Any) -> AttemptData:
        data = AttemptData(
            bake_id=secrets.token_hex(8),
            number=secrets.randbits(20),
            when=datetime.now(timezone.utc),
            result=TaskStatus.PENDING,
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
        return data

    def item_factory(self, **kwargs: Any) -> Attempt:
        data = self.data_factory()
        item = Attempt.from_data_obj(secrets.token_hex(8), data)
        for key, value in kwargs.items():
            item = replace(item, **{key: value})
        return item

    def compare_data(self, data1: AttemptData, data2: AttemptData) -> bool:
        return AttemptData.__eq__(data1, data2)

    async def test_create_get_update(self, storage: AttemptStorage) -> None:
        data = self.data_factory()
        created = await storage.create(data)
        assert self.compare_data(data, created)
        res = await storage.get(created.id)
        assert self.compare_data(res, created)
        assert res.id == created.id
        updated = replace(res, bake_id="new-bake-id")
        await storage.update(updated)
        project = await storage.get(res.id)
        assert project.bake_id == updated.bake_id

    async def test_get_not_exists(self, storage: AttemptStorage) -> None:
        with pytest.raises(NotExistsError):
            await storage.get("wrong-id")

    async def test_update_not_exists(self, storage: AttemptStorage) -> None:
        with pytest.raises(NotExistsError):
            item = self.item_factory()
            await storage.update(item)

    async def test_get_by_number(self, storage: AttemptStorage) -> None:
        data = self.data_factory()
        res = await storage.create(data)
        project = await storage.get_by_number(
            bake_id=data.bake_id,
            number=data.number,
        )
        assert project.id == res.id

    async def test_cannot_create_duplicate(self, storage: AttemptStorage) -> None:
        data = self.data_factory()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_list(self, storage: AttemptStorage) -> None:
        for bake_id in range(5):
            for number in range(5):
                data = self.data_factory(
                    bake_id=f"bake-id-{bake_id}",
                    number=number,
                )
                await storage.create(data)
        found = []
        async for item in storage.list(bake_id="bake-id-2"):
            found.append(item.number)
        assert len(found) == 5
        assert set(found) == {index for index in range(5)}


class TestTaskStorage:
    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> TaskStorage:
        return in_memory_storage.tasks

    def data_factory(self, **kwargs: Any) -> TaskData:
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
        return data

    def item_factory(self, **kwargs: Any) -> Task:
        data = self.data_factory()
        item = Task.from_data_obj(secrets.token_hex(8), data)
        for key, value in kwargs.items():
            item = replace(item, **{key: value})
        return item

    def compare_data(self, data1: TaskData, data2: TaskData) -> bool:
        return TaskData.__eq__(data1, data2)

    async def test_create_get_update(self, storage: TaskStorage) -> None:
        data = self.data_factory()
        created = await storage.create(data)
        assert self.compare_data(data, created)
        res = await storage.get(created.id)
        assert self.compare_data(res, created)
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
            item = self.item_factory()
            await storage.update(item)

    async def test_get_by_yaml_id(self, storage: TaskStorage) -> None:
        data = self.data_factory()
        res = await storage.create(data)
        project = await storage.get_by_yaml_id(
            attempt_id=data.attempt_id,
            yaml_id=data.yaml_id,
        )
        assert project.id == res.id

    async def test_cannot_create_duplicate(self, storage: TaskStorage) -> None:
        data = self.data_factory()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_list(self, storage: TaskStorage) -> None:
        for attempt_id in range(5):
            for raw_id in range(5):
                data = self.data_factory(
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
    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> CacheEntryStorage:
        return in_memory_storage.cache_entries

    def data_factory(self, **kwargs: Any) -> CacheEntryData:
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
        return data

    def item_factory(self, **kwargs: Any) -> CacheEntry:
        data = self.data_factory()
        item = CacheEntry.from_data_obj(secrets.token_hex(8), data)
        for key, value in kwargs.items():
            item = replace(item, **{key: value})
        return item

    def compare_data(self, data1: CacheEntryData, data2: CacheEntryData) -> bool:
        return CacheEntryData.__eq__(data1, data2)

    async def test_create_get_update(self, storage: CacheEntryStorage) -> None:
        data = self.data_factory()
        created = await storage.create(data)
        assert self.compare_data(data, created)
        res = await storage.get(created.id)
        assert self.compare_data(res, created)
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
            item = self.item_factory()
            await storage.update(item)

    async def test_get_by_yaml_id(self, storage: CacheEntryStorage) -> None:
        data = self.data_factory()
        res = await storage.create(data)
        item = await storage.get_by_key(
            task_id=data.task_id,
            project_id=data.project_id,
            batch=data.batch,
            key=data.key,
        )
        assert item.id == res.id

    async def test_cannot_create_duplicate(self, storage: CacheEntryStorage) -> None:
        data = self.data_factory()
        await storage.create(data)
        with pytest.raises(ExistsError):
            await storage.create(data)

    async def test_delete_all(self, storage: CacheEntryStorage) -> None:
        for key_id in range(5):
            for batch_id in range(5):
                for task_id in range(5):
                    for project_id in range(5):
                        data = self.data_factory(
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
    @pytest.fixture
    def storage(self, in_memory_storage: InMemoryStorage) -> ConfigFileStorage:
        return in_memory_storage.config_files

    def data_factory(self, **kwargs: Any) -> ConfigFileData:
        data = ConfigFileData(
            filename=secrets.token_hex(8),
            content=secrets.token_hex(8),
        )
        # Updating this way so constructor call is typechecked properly
        for key, value in kwargs.items():
            data = replace(data, **{key: value})
        return data

    def item_factory(self, **kwargs: Any) -> ConfigFile:
        data = self.data_factory()
        item = ConfigFile.from_data_obj(secrets.token_hex(8), data)
        for key, value in kwargs.items():
            item = replace(item, **{key: value})
        return item

    def compare_data(self, data1: ConfigFileData, data2: ConfigFileData) -> bool:
        return ConfigFileData.__eq__(data1, data2)

    async def test_create_get_update(self, storage: ConfigFileStorage) -> None:
        data = self.data_factory()
        created = await storage.create(data)
        assert self.compare_data(data, created)
        res = await storage.get(created.id)
        assert self.compare_data(res, created)
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
            item = self.item_factory()
            await storage.update(item)
