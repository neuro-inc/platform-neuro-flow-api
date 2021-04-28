import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, AsyncIterator, Callable, Dict, Optional, Sequence, TypeVar

import asyncpgsa
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
import sqlalchemy.sql as sasql
from asyncpg import Connection, UniqueViolationError
from asyncpg.cursor import CursorFactory
from asyncpg.pool import Pool
from asyncpg.protocol.protocol import Record

from .base import (
    Attempt,
    AttemptData,
    AttemptStorage,
    Bake,
    BakeData,
    BakeStorage,
    BaseStorage,
    CacheEntry,
    CacheEntryData,
    CacheEntryStorage,
    ConfigFile,
    ConfigFileData,
    ConfigFileStorage,
    ConfigsMeta,
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
    TaskStatusItem,
    TaskStorage,
)


def _full_id2str(full_id: FullID) -> str:
    return ".".join(full_id)


def _str2full_id(full_id: str) -> FullID:
    return tuple(full_id.split("."))


@dataclass(frozen=True)
class FlowTables:
    projects: sa.Table
    live_jobs: sa.Table
    bakes: sa.Table
    config_files: sa.Table
    attempts: sa.Table
    tasks: sa.Table
    cache_entries: sa.Table

    @classmethod
    def create(cls) -> "FlowTables":
        metadata = sa.MetaData()
        projects_table = sa.Table(
            "projects",
            metadata,
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("name", sa.String(), nullable=False),
            sa.Column("owner", sa.String(), nullable=False),
            sa.Column("cluster", sa.String(), nullable=False),
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        live_jobs_table = sa.Table(
            "live_jobs",
            metadata,
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column("yaml_id", sa.String(), nullable=False),
            sa.Column(
                "project_id",
                sa.String(),
                sa.ForeignKey(projects_table.c.id),
                nullable=False,
            ),
            sa.Column("tags", sapg.JSONB(), nullable=False),
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        bakes_table = sa.Table(
            "bakes",
            metadata,
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column(
                "project_id",
                sa.String(),
                sa.ForeignKey(projects_table.c.id),
                nullable=False,
            ),
            sa.Column("batch", sa.String(), nullable=False),
            sa.Column(
                "created_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=False
            ),
            sa.Column("tags", sapg.JSONB(), nullable=True),
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        config_files_table = sa.Table(
            "config_files",
            metadata,
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column(
                "bake_id", sa.String(), sa.ForeignKey("bakes.id"), nullable=False
            ),
            sa.Column("filename", sa.String(), nullable=False),
            sa.Column("content", sa.Text(), nullable=False),
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        attempts_table = sa.Table(
            "attempts",
            metadata,
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column(
                "bake_id", sa.String(), sa.ForeignKey(bakes_table.c.id), nullable=False
            ),
            sa.Column("number", sa.Integer(), nullable=False),
            sa.Column(
                "created_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=False
            ),
            sa.Column("result", sa.String(), nullable=False),
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        tasks_table = sa.Table(
            "tasks",
            metadata,
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column(
                "attempt_id",
                sa.String(),
                sa.ForeignKey(attempts_table.c.id),
                nullable=False,
            ),
            sa.Column("yaml_id", sa.String(), nullable=False),
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        cache_entries_table = sa.Table(
            "cache_entries",
            metadata,
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column(
                "project_id",
                sa.String(),
                sa.ForeignKey(projects_table.c.id),
                nullable=False,
            ),
            sa.Column("batch", sa.String(), nullable=False),
            sa.Column("task_id", sa.String(), nullable=False),
            sa.Column("key", sa.String(), nullable=False),
            sa.Column(
                "created_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=False
            ),
            sa.Column("payload", sapg.JSONB(), nullable=False),
        )
        return cls(
            projects=projects_table,
            live_jobs=live_jobs_table,
            bakes=bakes_table,
            config_files=config_files_table,
            attempts=attempts_table,
            tasks=tasks_table,
            cache_entries=cache_entries_table,
        )


_D = TypeVar("_D")
_E = TypeVar("_E", bound=HasId)


class BasePostgresStorage(BaseStorage[_D, _E], ABC):
    def __init__(
        self,
        table: sa.Table,
        pool: Pool,
        id_prefix: str,
        make_entry: Callable[[str, _D], _E],
    ):
        self._table = table
        self._pool = pool
        self._id_prefix = id_prefix
        self._make_entry = make_entry

    async def _execute(self, query: sasql.ClauseElement) -> str:
        query_string, params = asyncpgsa.compile_query(query)
        return await self._pool.execute(query_string, *params)

    async def _fetchrow(
        self,
        query: sasql.ClauseElement,
    ) -> Optional[Record]:
        query_string, params = asyncpgsa.compile_query(query)
        return await self._pool.fetchrow(query_string, *params)

    def _cursor(self, query: sasql.ClauseElement, conn: Connection) -> CursorFactory:
        query_string, params = asyncpgsa.compile_query(query)
        return conn.cursor(query_string, *params)

    def _gen_id(self) -> str:
        return f"{self._id_prefix}-{uuid.uuid4()}"

    @abstractmethod
    def _to_values(self, item: _E) -> Dict[str, Any]:
        pass

    @abstractmethod
    def _from_record(self, record: Record) -> _E:
        pass

    async def insert(self, data: _E) -> None:
        values = self._to_values(data)
        query = self._table.insert().values(values)
        try:
            await self._execute(query)
        except UniqueViolationError:
            raise ExistsError
        return None

    async def create(self, data: _D) -> _E:
        entry = self._make_entry(self._gen_id(), data)
        values = self._to_values(entry)
        query = self._table.insert().values(values)
        try:
            await self._execute(query)
        except UniqueViolationError:
            raise ExistsError
        return entry

    async def update(self, data: _E) -> None:
        values = self._to_values(data)
        id = values.pop("id")
        query = (
            self._table.update()
            .values(values)
            .where(self._table.c.id == id)
            .returning(self._table.c.id)
        )
        result = await self._fetchrow(query)
        if not result:
            # Docs on status messages are placed here:
            # https://www.postgresql.org/docs/current/protocol-message-formats.html
            raise NotExistsError

    async def get(self, id: str) -> _E:
        query = self._table.select(self._table.c.id == id)
        record = await self._fetchrow(query)
        if not record:
            raise NotExistsError
        return self._from_record(record)


class PostgresProjectStorage(ProjectStorage, BasePostgresStorage[ProjectData, Project]):
    def _to_values(self, item: Project) -> Dict[str, Any]:
        payload = asdict(item)
        return {
            "id": payload.pop("id"),
            "name": payload.pop("name"),
            "owner": payload.pop("owner"),
            "cluster": payload.pop("cluster"),
            "payload": payload,
        }

    def _from_record(self, record: Record) -> Project:
        payload = json.loads(record["payload"])
        payload["id"] = record["id"]
        payload["name"] = record["name"]
        payload["owner"] = record["owner"]
        payload["cluster"] = record["cluster"]
        return Project(**payload)

    async def get_by_name(self, name: str, owner: str, cluster: str) -> Project:
        query = (
            self._table.select()
            .where(self._table.c.name == name)
            .where(self._table.c.owner == owner)
            .where(self._table.c.cluster == cluster)
        )
        record = await self._fetchrow(query)
        if not record:
            raise NotExistsError
        return self._from_record(record)

    async def list(
        self,
        name: Optional[str] = None,
        owner: Optional[str] = None,
        cluster: Optional[str] = None,
    ) -> AsyncIterator[Project]:
        query = self._table.select()
        if name is not None:
            query = query.where(self._table.c.name == name)
        if owner is not None:
            query = query.where(self._table.c.owner == owner)
        if cluster is not None:
            query = query.where(self._table.c.cluster == cluster)
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in self._cursor(query, conn=conn):
                yield self._from_record(record)


class PostgresLiveJobsStorage(
    LiveJobStorage, BasePostgresStorage[LiveJobData, LiveJob]
):
    def _to_values(self, item: LiveJob) -> Dict[str, Any]:
        payload = asdict(item)
        return {
            "id": payload.pop("id"),
            "yaml_id": payload.pop("yaml_id"),
            "project_id": payload.pop("project_id"),
            "tags": payload.pop("tags"),
            "payload": payload,
        }

    def _from_record(self, record: Record) -> LiveJob:
        payload = json.loads(record["payload"])
        payload["id"] = record["id"]
        payload["yaml_id"] = record["yaml_id"]
        payload["project_id"] = record["project_id"]
        payload["tags"] = json.loads(record["tags"])
        return LiveJob(**payload)

    async def get_by_yaml_id(self, yaml_id: str, project_id: str) -> LiveJob:
        query = (
            self._table.select()
            .where(self._table.c.yaml_id == yaml_id)
            .where(self._table.c.project_id == project_id)
        )
        record = await self._fetchrow(query)
        if not record:
            raise NotExistsError
        return self._from_record(record)

    async def update_or_create(self, data: LiveJobData) -> LiveJob:
        try:
            job = await self.get_by_yaml_id(data.yaml_id, data.project_id)
        except NotExistsError:
            return await self.create(data)
        else:
            job = self._make_entry(job.id, data)
            await self.update(job)
            return job

    async def list(
        self,
        project_id: Optional[str] = None,
    ) -> AsyncIterator[LiveJob]:
        query = self._table.select()
        if project_id is not None:
            query = query.where(self._table.c.project_id == project_id)
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in self._cursor(query, conn=conn):
                yield self._from_record(record)


class PostgresBakeStorage(BakeStorage, BasePostgresStorage[BakeData, Bake]):
    def _to_values(self, item: Bake) -> Dict[str, Any]:
        payload = asdict(item)
        graphs = {}
        for key, subgraph in item.graphs.items():
            subgr = {}
            for node, deps in subgraph.items():
                subgr[_full_id2str(node)] = [_full_id2str(dep) for dep in deps]
            graphs[_full_id2str(key)] = subgr
        payload["graphs"] = graphs
        return {
            "id": payload.pop("id"),
            "project_id": payload.pop("project_id"),
            "batch": payload.pop("batch"),
            "created_at": payload.pop("created_at"),
            "tags": payload.pop("tags"),
            "payload": payload,
        }

    def _from_record(self, record: Record) -> Bake:
        payload = json.loads(record["payload"])
        payload["id"] = record["id"]
        payload["project_id"] = record["project_id"]
        payload["batch"] = record["batch"]
        payload["created_at"] = record["created_at"]
        payload["tags"] = json.loads(record["tags"])
        graphs = {}
        for key, subgraph in payload["graphs"].items():
            subgr = {}
            for node, deps in subgraph.items():
                subgr[_str2full_id(node)] = {_str2full_id(dep) for dep in deps}
            graphs[_str2full_id(key)] = subgr
        payload["graphs"] = graphs
        return Bake(**payload)

    async def list(
        self,
        project_id: Optional[str] = None,
        tags: Optional[Sequence[str]] = None,
    ) -> AsyncIterator[Bake]:
        query = self._table.select()
        if project_id is not None:
            query = query.where(self._table.c.project_id == project_id)
        if tags is not None:
            query = query.where(self._table.c.tags.contains(list(tags)))
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in self._cursor(query, conn=conn):
                yield self._from_record(record)


class PostgresAttemptStorage(AttemptStorage, BasePostgresStorage[AttemptData, Attempt]):
    def _to_values(self, item: Attempt) -> Dict[str, Any]:
        payload = asdict(item)
        return {
            "id": payload.pop("id"),
            "bake_id": payload.pop("bake_id"),
            "number": payload.pop("number"),
            "created_at": payload.pop("created_at"),
            "result": payload.pop("result"),
            "payload": payload,
        }

    def _from_record(self, record: Record) -> Attempt:
        payload = json.loads(record["payload"])
        payload["id"] = record["id"]
        payload["bake_id"] = record["bake_id"]
        payload["number"] = record["number"]
        payload["created_at"] = record["created_at"]
        payload["result"] = TaskStatus(record["result"])
        payload["configs_meta"] = ConfigsMeta(**payload["configs_meta"])
        return Attempt(**payload)

    async def get_by_number(self, bake_id: str, number: int) -> Attempt:
        if number == -1:
            query = (
                self._table.select()
                .where(self._table.c.bake_id == bake_id)
                .order_by(self._table.c.number.desc())
                .limit(1)
            )
        else:
            query = (
                self._table.select()
                .where(self._table.c.bake_id == bake_id)
                .where(self._table.c.number == number)
            )
        record = await self._fetchrow(query)
        if not record:
            raise NotExistsError
        return self._from_record(record)

    async def list(
        self,
        bake_id: Optional[str] = None,
    ) -> AsyncIterator[Attempt]:
        query = self._table.select()
        if bake_id is not None:
            query = query.where(self._table.c.bake_id == bake_id)
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in self._cursor(query, conn=conn):
                yield self._from_record(record)


class PostgresTaskStorage(TaskStorage, BasePostgresStorage[TaskData, Task]):
    def _to_values(self, item: Task) -> Dict[str, Any]:
        payload = asdict(item)
        payload["statuses"] = [
            {
                "when": status.when.isoformat(),
                "status": status.status.value,
            }
            for status in item.statuses
        ]
        return {
            "id": payload.pop("id"),
            "attempt_id": payload.pop("attempt_id"),
            "yaml_id": _full_id2str(payload.pop("yaml_id")),
            "payload": payload,
        }

    def _from_record(self, record: Record) -> Task:
        payload = json.loads(record["payload"])
        payload["id"] = record["id"]
        payload["attempt_id"] = record["attempt_id"]
        payload["yaml_id"] = _str2full_id(record["yaml_id"])
        payload["statuses"] = [
            TaskStatusItem(
                when=datetime.fromisoformat(item["when"]),
                status=TaskStatus(item["status"]),
            )
            for item in payload["statuses"]
        ]
        return Task(**payload)

    async def get_by_yaml_id(self, yaml_id: FullID, attempt_id: str) -> Task:
        query = (
            self._table.select()
            .where(self._table.c.yaml_id == _full_id2str(yaml_id))
            .where(self._table.c.attempt_id == attempt_id)
        )
        record = await self._fetchrow(query)
        if not record:
            raise NotExistsError
        return self._from_record(record)

    async def list(
        self,
        attempt_id: Optional[str] = None,
    ) -> AsyncIterator[Task]:
        query = self._table.select()
        if attempt_id is not None:
            query = query.where(self._table.c.attempt_id == attempt_id)
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in self._cursor(query, conn=conn):
                yield self._from_record(record)


class PostgresCacheEntryStorage(
    CacheEntryStorage, BasePostgresStorage[CacheEntryData, CacheEntry]
):
    def _to_values(self, item: CacheEntry) -> Dict[str, Any]:
        payload = asdict(item)
        return {
            "id": payload.pop("id"),
            "project_id": payload.pop("project_id"),
            "task_id": _full_id2str(payload.pop("task_id")),
            "batch": payload.pop("batch"),
            "key": payload.pop("key"),
            "created_at": payload.pop("created_at"),
            "payload": payload,
        }

    def _from_record(self, record: Record) -> CacheEntry:
        payload = json.loads(record["payload"])
        payload["id"] = record["id"]
        payload["project_id"] = record["project_id"]
        payload["task_id"] = _str2full_id(record["task_id"])
        payload["batch"] = record["batch"]
        payload["key"] = record["key"]
        payload["created_at"] = record["created_at"]
        return CacheEntry(**payload)

    async def get_by_key(
        self, project_id: str, task_id: FullID, batch: str, key: str
    ) -> CacheEntry:
        query = (
            self._table.select()
            .where(self._table.c.project_id == project_id)
            .where(self._table.c.task_id == _full_id2str(task_id))
            .where(self._table.c.batch == batch)
            .where(self._table.c.key == key)
        )
        record = await self._fetchrow(query)
        if not record:
            raise NotExistsError
        return self._from_record(record)

    async def delete_all(
        self,
        project_id: Optional[str] = None,
        task_id: Optional[FullID] = None,
        batch: Optional[str] = None,
    ) -> None:
        query = self._table.delete()
        if project_id is not None:
            query = query.where(self._table.c.project_id == project_id)
        if task_id is not None:
            query = query.where(self._table.c.task_id == _full_id2str(task_id))
        if batch is not None:
            query = query.where(self._table.c.batch == batch)
        await self._execute(query)


class PostgresConfigFileStorage(
    ConfigFileStorage, BasePostgresStorage[ConfigFileData, ConfigFile]
):
    def _to_values(self, item: ConfigFile) -> Dict[str, Any]:
        payload = asdict(item)
        return {
            "id": payload.pop("id"),
            "bake_id": payload.pop("bake_id"),
            "filename": payload.pop("filename"),
            "content": payload.pop("content"),
            "payload": payload,
        }

    def _from_record(self, record: Record) -> ConfigFile:
        payload = json.loads(record["payload"])
        payload["id"] = record["id"]
        payload["bake_id"] = record["bake_id"]
        payload["filename"] = record["filename"]
        payload["content"] = record["content"]
        return ConfigFile(**payload)


class PostgresStorage(Storage):
    projects: PostgresProjectStorage

    def __init__(self, pool: Pool) -> None:
        tables = FlowTables.create()
        self.projects = PostgresProjectStorage(
            table=tables.projects,
            pool=pool,
            id_prefix="projects",
            make_entry=Project.from_data_obj,
        )
        self.live_jobs = PostgresLiveJobsStorage(
            table=tables.live_jobs,
            pool=pool,
            id_prefix="live-job",
            make_entry=LiveJob.from_data_obj,
        )
        self.bakes = PostgresBakeStorage(
            table=tables.bakes,
            pool=pool,
            id_prefix="bake",
            make_entry=Bake.from_data_obj,
        )
        self.attempts = PostgresAttemptStorage(
            table=tables.attempts,
            pool=pool,
            id_prefix="attempt",
            make_entry=Attempt.from_data_obj,
        )
        self.tasks = PostgresTaskStorage(
            table=tables.tasks,
            pool=pool,
            id_prefix="task",
            make_entry=Task.from_data_obj,
        )
        self.cache_entries = PostgresCacheEntryStorage(
            table=tables.cache_entries,
            pool=pool,
            id_prefix="cache-entry",
            make_entry=CacheEntry.from_data_obj,
        )
        self.config_files = PostgresConfigFileStorage(
            table=tables.config_files,
            pool=pool,
            id_prefix="config-file",
            make_entry=ConfigFile.from_data_obj,
        )
