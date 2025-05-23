from __future__ import annotations

import secrets
from collections.abc import AsyncIterator, Callable, Set as AbstractSet
from dataclasses import replace
from datetime import datetime
from typing import TypeVar

from .base import (
    Attempt,
    AttemptData,
    AttemptStorage,
    Bake,
    BakeData,
    BakeImage,
    BakeImageData,
    BakeImageStorage,
    BakeStorage,
    BaseStorage,
    CacheEntry,
    CacheEntryData,
    CacheEntryStorage,
    ConfigFile,
    ConfigFileData,
    ConfigFileStorage,
    ExistsError,
    FullID,
    HasId,
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
    TaskStorage,
    _Sentinel,
    sentinel,
)

_D = TypeVar("_D")
_E = TypeVar("_E", bound=HasId)


class InMemoryBaseStorage(BaseStorage[_D, _E]):
    def __init__(self, make_entity: Callable[[str, _D], _E]) -> None:
        self._items: dict[str, _E] = {}
        self._make_entry = make_entity

    def _gen_id(self) -> str:
        return secrets.token_hex(8)

    async def insert(self, data: _E) -> None:
        await self.check_exists(data)  # type: ignore
        if data.id in self._items:
            raise ExistsError
        self._items[data.id] = data

    async def create(self, data: _D) -> _E:
        await self.check_exists(data)
        new_id = self._gen_id()
        entity = self._make_entry(new_id, data)
        self._items[new_id] = entity
        return entity

    async def update(self, data: _E) -> None:
        if data.id not in self._items:
            raise NotExistsError
        self._items[data.id] = data

    async def get(self, id: str) -> _E:
        if id not in self._items:
            raise NotExistsError
        return self._items[id]

    async def check_exists(self, data: _D) -> None:
        pass

    async def delete(self, id: str) -> None:
        if id not in self._items:
            raise NotExistsError
        self._items.pop(id)


class InMemoryProjectStorage(ProjectStorage, InMemoryBaseStorage[ProjectData, Project]):
    async def check_exists(self, data: ProjectData) -> None:
        try:
            await self.get_by_name(
                data.name, data.project_name, data.cluster, data.org_name
            )
        except NotExistsError:
            return
        raise ExistsError

    async def get_by_name(
        self, name: str, project_name: str, cluster: str, org_name: str | None
    ) -> Project:
        for item in self._items.values():
            if (
                item.name == name
                and item.project_name == project_name
                and item.cluster == cluster
                and item.org_name == org_name
            ):
                return item
        raise NotExistsError

    async def list(
        self,
        name: str | None = None,
        owner: str | None = None,
        project_name: str | None = None,
        cluster: str | None = None,
        org_name: _Sentinel | str | None = sentinel,
    ) -> AsyncIterator[Project]:
        for item in self._items.values():
            if name is not None and item.name != name:
                continue
            if owner is not None and item.owner != owner:
                continue
            if project_name is not None and item.project_name != project_name:
                continue
            if cluster is not None and item.cluster != cluster:
                continue
            if org_name is not sentinel and item.org_name != org_name:
                continue
            yield item


class InMemoryLiveJobStorage(LiveJobStorage, InMemoryBaseStorage[LiveJobData, LiveJob]):
    async def check_exists(self, data: LiveJobData) -> None:
        try:
            await self.get_by_yaml_id(data.yaml_id, data.project_id)
        except NotExistsError:
            return
        raise ExistsError

    async def get_by_yaml_id(self, yaml_id: str, project_id: str) -> LiveJob:
        for item in self._items.values():
            if item.yaml_id == yaml_id and item.project_id == project_id:
                return item
        raise NotExistsError

    async def update_or_create(self, data: LiveJobData) -> LiveJob:
        try:
            job = await self.get_by_yaml_id(data.yaml_id, data.project_id)
        except NotExistsError:
            return await self.create(data)
        else:
            job = self._make_entry(job.id, data)
            await self.update(job)
            return job

    async def list(self, project_id: str | None = None) -> AsyncIterator[LiveJob]:
        for item in self._items.values():
            if project_id is not None and item.project_id != project_id:
                continue
            yield item


class InMemoryAttemptStorage(AttemptStorage, InMemoryBaseStorage[AttemptData, Attempt]):
    async def check_exists(self, data: AttemptData) -> None:
        try:
            await self.get_by_number(data.bake_id, data.number)
        except NotExistsError:
            return
        raise ExistsError

    async def get_by_number(self, bake_id: str, number: int) -> Attempt:
        if number == -1:
            found = None
            # get the last attempt
            for item in self._items.values():
                if item.bake_id == bake_id:
                    if found is None or item.number > found.number:
                        found = item
            if found is not None:
                return found
        else:
            for item in self._items.values():
                if item.bake_id == bake_id and item.number == number:
                    return item
        raise NotExistsError

    async def list(
        self, bake_id: str | None = None, results: AbstractSet[TaskStatus] | None = None
    ) -> AsyncIterator[Attempt]:
        for item in self._items.values():
            if bake_id is not None and item.bake_id != bake_id:
                continue
            if results is not None and item.result not in results:
                continue
            yield item


class InMemoryTaskStorage(TaskStorage, InMemoryBaseStorage[TaskData, Task]):
    async def check_exists(self, data: TaskData) -> None:
        try:
            await self.get_by_yaml_id(data.yaml_id, data.attempt_id)
        except NotExistsError:
            return
        raise ExistsError

    async def get_by_yaml_id(self, yaml_id: FullID, attempt_id: str) -> Task:
        for item in self._items.values():
            if item.yaml_id == yaml_id and item.attempt_id == attempt_id:
                return item
        raise NotExistsError

    async def list(self, attempt_id: str | None = None) -> AsyncIterator[Task]:
        for item in self._items.values():
            if attempt_id is not None and item.attempt_id != attempt_id:
                continue
            yield item


class InMemoryBakeStorage(BakeStorage, InMemoryBaseStorage[BakeData, Bake]):
    def __init__(
        self, make_entity: Callable[[str, BakeData], Bake], attempts: AttemptStorage
    ) -> None:
        super().__init__(make_entity)
        self.attempts = attempts

    async def list(  # noqa: C901
        self,
        project_id: str | None = None,
        name: str | None = None,
        tags: AbstractSet[str] = frozenset(),
        *,
        fetch_last_attempt: bool = False,
        since: datetime | None = None,
        until: datetime | None = None,
        reverse: bool = False,
    ) -> AsyncIterator[Bake]:
        unsorted: list[Bake] = []
        for item in self._items.values():
            if project_id is not None and item.project_id != project_id:
                continue
            if name is not None and item.name != name:
                continue
            if not set(tags).issubset(set(item.tags)):
                continue
            if since is not None and item.created_at <= since:
                continue
            if until is not None and item.created_at >= until:
                continue
            if not fetch_last_attempt:
                unsorted.append(item)
            else:
                try:
                    attempt = await self.attempts.get_by_number(item.id, number=-1)
                    unsorted.append(replace(item, last_attempt=attempt))
                except NotExistsError:
                    unsorted.append(item)
        res = sorted(unsorted, key=lambda it: it.created_at)
        if reverse:
            res = list(reversed(res))
        for item in res:
            yield item

    async def get(self, id: str, *, fetch_last_attempt: bool = False) -> Bake:
        ret = self._items.get(id)
        if ret is None:
            raise NotExistsError
        if fetch_last_attempt:
            try:
                attempt = await self.attempts.get_by_number(ret.id, number=-1)
                return replace(ret, last_attempt=attempt)
            except NotExistsError:
                return ret
        return ret

    async def get_by_name(
        self, project_id: str, name: str, *, fetch_last_attempt: bool = False
    ) -> Bake:
        raise NotImplementedError("Name uniqueness is not enforced for InMemoryStorage")


class InMemoryCacheEntryStorage(
    CacheEntryStorage, InMemoryBaseStorage[CacheEntryData, CacheEntry]
):
    async def check_exists(self, data: CacheEntryData) -> None:
        try:
            await self.get_by_key(data.project_id, data.task_id, data.batch, data.key)
        except NotExistsError:
            return
        raise ExistsError

    async def get_by_key(
        self, project_id: str, task_id: FullID, batch: str, key: str
    ) -> CacheEntry:
        for item in self._items.values():
            if (
                item.project_id == project_id
                and item.task_id == task_id
                and item.batch == batch
                and item.key == key
            ):
                return item
        raise NotExistsError

    async def delete_all(
        self,
        project_id: str | None = None,
        task_id: FullID | None = None,
        batch: str | None = None,
    ) -> None:
        new_items = {}
        for item in self._items.values():
            if project_id is not None and item.project_id == project_id:
                continue
            if task_id is not None and item.task_id == task_id:
                continue
            if batch is not None and item.batch == batch:
                continue
            new_items[item.id] = item
        self._items = new_items


class InMemoryConfigFileStorage(
    ConfigFileStorage, InMemoryBaseStorage[ConfigFileData, ConfigFile]
):
    pass


class InMemoryBakeImageStorage(
    BakeImageStorage, InMemoryBaseStorage[BakeImageData, BakeImage]
):
    async def check_exists(self, data: BakeImageData) -> None:
        try:
            await self.get_by_ref(data.bake_id, data.ref)
        except NotExistsError:
            return
        raise ExistsError

    async def get_by_ref(self, bake_id: str, ref: str) -> BakeImage:
        for item in self._items.values():
            if item.bake_id == bake_id and item.ref == ref:
                return item
        raise NotExistsError

    async def list(self, bake_id: str | None = None) -> AsyncIterator[BakeImage]:
        for item in self._items.values():
            if bake_id is not None and item.bake_id != bake_id:
                continue
            yield item


class InMemoryStorage(Storage):
    def __init__(self) -> None:
        self.projects = InMemoryProjectStorage(Project.from_data_obj)
        self.live_jobs = InMemoryLiveJobStorage(LiveJob.from_data_obj)
        self.attempts = InMemoryAttemptStorage(Attempt.from_data_obj)
        self.tasks = InMemoryTaskStorage(Task.from_data_obj)
        self.bakes = InMemoryBakeStorage(Bake.from_data_obj, self.attempts)
        self.cache_entries = InMemoryCacheEntryStorage(CacheEntry.from_data_obj)
        self.config_files = InMemoryConfigFileStorage(ConfigFile.from_data_obj)
        self.bake_images = InMemoryBakeImageStorage(BakeImage.from_data_obj)
