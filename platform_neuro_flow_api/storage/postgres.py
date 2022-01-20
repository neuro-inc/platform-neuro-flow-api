from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable, Set
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, TypeVar

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sapg
import sqlalchemy.sql as sasql
from asyncpg import UniqueViolationError
from neuro_logging import trace
from sqlalchemy import asc, desc
from sqlalchemy.engine import Row
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .base import (
    Attempt,
    AttemptData,
    AttemptStorage,
    Bake,
    BakeData,
    BakeImage,
    BakeImageData,
    BakeImageStorage,
    BakeMeta,
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
    GitInfo,
    HasId,
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
    UniquenessError,
)


def _full_id2str(full_id: FullID) -> str:
    return ".".join(full_id)


def _str2full_id(full_id: str) -> FullID:
    return () if full_id == "" else tuple(full_id.split("."))


@dataclass(frozen=True)
class FlowTables:
    projects: sa.Table
    live_jobs: sa.Table
    bakes: sa.Table
    config_files: sa.Table
    attempts: sa.Table
    tasks: sa.Table
    cache_entries: sa.Table
    bake_images: sa.Table

    @classmethod
    def create(cls) -> FlowTables:
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
            sa.Column("name", sa.String(), nullable=True),
            sa.Column(
                "created_at", sapg.TIMESTAMP(timezone=True, precision=6), nullable=False
            ),
            sa.Column("status", sa.String(), nullable=False, server_default="pending"),
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
        bake_images_table = sa.Table(
            "bake_images",
            metadata,
            sa.Column("id", sa.String(), primary_key=True),
            sa.Column(
                "bake_id", sa.String(), sa.ForeignKey(bakes_table.c.id), nullable=False
            ),
            sa.Column("ref", sa.String(), nullable=False),
            sa.Column("status", sa.String(), nullable=False),
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
            bake_images=bake_images_table,
        )


_D = TypeVar("_D")
_E = TypeVar("_E", bound=HasId)


class BasePostgresStorage(BaseStorage[_D, _E], ABC):
    def __init__(
        self,
        table: sa.Table,
        engine: AsyncEngine,
        id_prefix: str,
        make_entry: Callable[[str, _D], _E],
    ):
        self._table = table
        self._engine = engine
        self._id_prefix = id_prefix
        self._make_entry = make_entry

    async def _execute(
        self, query: sasql.ClauseElement, conn: AsyncConnection | None = None
    ) -> None:
        if conn:
            await conn.execute(query)
            return
        async with self._engine.connect() as conn:
            await conn.execute(query)

    async def _fetchrow(
        self, query: sasql.ClauseElement, conn: AsyncConnection | None = None
    ) -> Row | None:
        if conn:
            result = await conn.execute(query)
            return result.one_or_none()
        async with self._engine.connect() as conn:
            result = await conn.execute(query)
            return result.one_or_none()

    async def _cursor(
        self, query: sasql.ClauseElement, conn: AsyncConnection
    ) -> AsyncIterator[Row]:
        return await conn.stream(query)

    def _gen_id(self) -> str:
        return f"{self._id_prefix}-{uuid.uuid4()}"

    @abstractmethod
    def _to_values(self, item: _E) -> dict[str, Any]:
        pass

    @abstractmethod
    def _from_record(self, record: Row) -> _E:
        pass

    async def after_insert(self, data: _E, conn: AsyncConnection) -> None:
        pass

    async def after_update(self, data: _E, conn: AsyncConnection) -> None:
        pass

    @trace
    async def insert(self, data: _E) -> None:
        values = self._to_values(data)
        query = self._table.insert().values(values)
        async with self._engine.begin() as conn:
            try:
                await self._execute(query, conn)
            except IntegrityError as exc:
                if isinstance(exc.orig.__cause__, UniqueViolationError):
                    raise ExistsError
                raise
            await self.after_insert(data, conn)

    @trace
    async def create(self, data: _D) -> _E:
        entry = self._make_entry(self._gen_id(), data)
        await self.insert(entry)
        return entry

    @trace
    async def update(self, data: _E) -> None:
        values = self._to_values(data)
        id = values.pop("id")
        query = (
            self._table.update()
            .values(values)
            .where(self._table.c.id == id)
            .returning(self._table.c.id)
        )
        async with self._engine.begin() as conn:
            result = await self._fetchrow(query, conn)
            if not result:
                # Docs on status messages are placed here:
                # https://www.postgresql.org/docs/current/protocol-message-formats.html
                raise NotExistsError
            await self.after_update(data, conn)

    @trace
    async def get(self, id: str) -> _E:
        query = self._table.select(self._table.c.id == id)
        record = await self._fetchrow(query)
        if not record:
            raise NotExistsError
        return self._from_record(record)

    @trace
    async def delete(self, id: str) -> None:
        query = (
            self._table.delete()
            .where(self._table.c.id == id)
            .returning(self._table.c.id)
        )
        async with self._engine.begin() as conn:
            record = await self._fetchrow(query, conn)
            if not record:
                raise NotExistsError


class PostgresProjectStorage(ProjectStorage, BasePostgresStorage[ProjectData, Project]):
    def _to_values(self, item: Project) -> dict[str, Any]:
        payload = asdict(item)
        return {
            "id": payload.pop("id"),
            "name": payload.pop("name"),
            "owner": payload.pop("owner"),
            "cluster": payload.pop("cluster"),
            "payload": payload,
        }

    def _from_record(self, record: Row) -> Project:
        payload = record["payload"]
        payload["id"] = record["id"]
        payload["name"] = record["name"]
        payload["owner"] = record["owner"]
        payload["cluster"] = record["cluster"]
        return Project(**payload)

    @trace
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
        name: str | None = None,
        owner: str | None = None,
        cluster: str | None = None,
    ) -> AsyncIterator[Project]:
        query = self._table.select()
        if name is not None:
            query = query.where(self._table.c.name == name)
        if owner is not None:
            query = query.where(self._table.c.owner == owner)
        if cluster is not None:
            query = query.where(self._table.c.cluster == cluster)
        async with self._engine.begin() as conn:
            async for record in await self._cursor(query, conn=conn):
                yield self._from_record(record)


class PostgresLiveJobsStorage(
    LiveJobStorage, BasePostgresStorage[LiveJobData, LiveJob]
):
    def _to_values(self, item: LiveJob) -> dict[str, Any]:
        payload = asdict(item)
        return {
            "id": payload.pop("id"),
            "yaml_id": payload.pop("yaml_id"),
            "project_id": payload.pop("project_id"),
            "tags": payload.pop("tags"),
            "payload": payload,
        }

    def _from_record(self, record: Row) -> LiveJob:
        payload = record["payload"]
        payload["id"] = record["id"]
        payload["yaml_id"] = record["yaml_id"]
        payload["project_id"] = record["project_id"]
        payload["tags"] = record["tags"]
        return LiveJob(**payload)

    @trace
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

    @trace
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
        project_id: str | None = None,
    ) -> AsyncIterator[LiveJob]:
        query = self._table.select()
        if project_id is not None:
            query = query.where(self._table.c.project_id == project_id)
        async with self._engine.begin() as conn:
            async for record in await self._cursor(query, conn=conn):
                yield self._from_record(record)


def _attempt_from_record(record: Row, use_labels: bool = False) -> Attempt:
    def key(name: str) -> str:
        if use_labels:
            return "attempts_" + name
        else:
            return name

    payload = record[key("payload")]
    payload["id"] = record[key("id")]
    payload["bake_id"] = record[key("bake_id")]
    payload["number"] = record[key("number")]
    payload["created_at"] = record[key("created_at")]
    payload["result"] = TaskStatus(record[key("result")])
    payload["configs_meta"] = ConfigsMeta(**payload["configs_meta"])
    return Attempt(**payload)


class PostgresBakeStorage(BakeStorage, BasePostgresStorage[BakeData, Bake]):
    def __init__(
        self,
        bakes_table: sa.Table,
        attempts_table: sa.Table,
        engine: AsyncEngine,
        id_prefix: str,
        make_entry: Callable[[str, BakeData], Bake],
    ):
        super().__init__(bakes_table, engine, id_prefix, make_entry)
        self._attempts_table = attempts_table

    def _to_values(self, item: Bake) -> dict[str, Any]:
        payload = asdict(item)
        graphs = {}
        for key, subgraph in item.graphs.items():
            subgr = {}
            for node, deps in subgraph.items():
                subgr[_full_id2str(node)] = [_full_id2str(dep) for dep in deps]
            graphs[_full_id2str(key)] = subgr
        payload["graphs"] = graphs
        payload["meta"] = {
            "git_info": {
                "sha": item.meta.git_info.sha,
                "branch": item.meta.git_info.branch,
                "tags": item.meta.git_info.tags,
            }
            if item.meta.git_info
            else None,
        }
        return {
            "id": payload.pop("id"),
            "project_id": payload.pop("project_id"),
            "batch": payload.pop("batch"),
            "created_at": payload.pop("created_at"),
            "name": payload.pop("name"),
            "tags": payload.pop("tags"),
            "payload": payload,
        }

    def _from_record(self, record: Row, fetch_last_attempt: bool = False) -> Bake:
        attempt = None
        if fetch_last_attempt:
            if record["attempts_id"] is not None:
                attempt = _attempt_from_record(record, use_labels=True)

        def key(name: str) -> str:
            if fetch_last_attempt:
                return "bakes_" + name
            else:
                return name

        payload = record[key("payload")]
        payload["id"] = record[key("id")]
        payload["project_id"] = record[key("project_id")]
        payload["batch"] = record[key("batch")]
        payload["created_at"] = record[key("created_at")]
        payload["name"] = record[key("name")]
        payload["tags"] = record[key("tags")] or []
        graphs = {}
        for grkey, subgraph in payload["graphs"].items():
            subgr = {}
            for node, deps in subgraph.items():
                subgr[_str2full_id(node)] = {_str2full_id(dep) for dep in deps}
            graphs[_str2full_id(grkey)] = subgr
        if "meta" in payload:
            git_info: GitInfo | None = None
            if payload["meta"].get("git_info"):
                git_info = GitInfo(
                    sha=payload["meta"]["git_info"]["sha"],
                    branch=payload["meta"]["git_info"]["branch"],
                    tags=payload["meta"]["git_info"]["tags"],
                )
            payload["meta"] = BakeMeta(git_info)
        payload["graphs"] = graphs
        payload["last_attempt"] = attempt
        return Bake(**payload)

    def _make_q(self, fetch_last_attempt: bool) -> sasql.Selectable:
        if fetch_last_attempt:
            join = self._table.outerjoin(
                self._attempts_table, self._table.c.id == self._attempts_table.c.bake_id
            )
            return sasql.select(
                [self._table, self._attempts_table], use_labels=True
            ).select_from(join)
        else:
            return self._table.select()

    def _filter_last_attempt(self, query: sasql.Selectable) -> sasql.Selectable:
        # Only select single bake with last attempt
        return query.distinct(self._table.c.id).order_by(
            self._table.c.id, desc(self._attempts_table.c.number)
        )

    async def list(
        self,
        project_id: str | None = None,
        name: str | None = None,
        tags: Set[str] = frozenset(),
        *,
        fetch_last_attempt: bool = False,
        since: datetime | None = None,
        until: datetime | None = None,
        reverse: bool = False,
    ) -> AsyncIterator[Bake]:
        query = self._make_q(fetch_last_attempt)
        if project_id is not None:
            query = query.where(self._table.c.project_id == project_id)
        if name is not None:
            query = query.where(self._table.c.name == name)
        if tags:
            query = query.where(self._table.c.tags.contains(list(tags)))
        if since:
            query = query.where(self._table.c.created_at >= since)
        if until:
            query = query.where(self._table.c.created_at <= until)
        if fetch_last_attempt:
            sub_query = self._filter_last_attempt(query).alias()
            # To allow reordering, we need subquery
            query = sasql.select(sub_query.c).select_from(sub_query)
            created_at_field = sub_query.c.bakes_created_at
        else:
            created_at_field = self._table.c.created_at
        if reverse:
            query = query.order_by(desc(created_at_field))
        else:
            query = query.order_by(asc(created_at_field))
        async with self._engine.begin() as conn:
            async for record in await self._cursor(query, conn=conn):
                yield self._from_record(record, fetch_last_attempt)

    @trace
    async def get(self, id: str, *, fetch_last_attempt: bool = False) -> Bake:
        query = self._make_q(fetch_last_attempt)
        query = query.where(self._table.c.id == id)
        if fetch_last_attempt:
            query = self._filter_last_attempt(query)
        record = await self._fetchrow(query)
        if not record:
            raise NotExistsError
        return self._from_record(record, fetch_last_attempt)

    @trace
    async def get_by_name(
        self,
        project_id: str,
        name: str,
        *,
        fetch_last_attempt: bool = False,
    ) -> Bake:
        query = (
            self._make_q(fetch_last_attempt)
            .where(self._table.c.project_id == project_id)
            .where(self._table.c.name == name)
        )
        if fetch_last_attempt:
            # To allow reordering, we need subquery
            sub_query = self._filter_last_attempt(query).alias()
            query = sasql.select(sub_query.c).select_from(sub_query)
            created_at_field = sub_query.c.bakes_created_at
        else:
            created_at_field = self._table.c.created_at
        query = query.order_by(desc(created_at_field)).limit(1)
        record = await self._fetchrow(query)
        if not record:
            raise NotExistsError
        return self._from_record(record, fetch_last_attempt)


class PostgresAttemptStorage(AttemptStorage, BasePostgresStorage[AttemptData, Attempt]):
    def __init__(
        self,
        attempts_table: sa.Table,
        bakes_table: sa.Table,
        engine: AsyncEngine,
        id_prefix: str,
        make_entry: Callable[[str, AttemptData], Attempt],
    ):
        super().__init__(attempts_table, engine, id_prefix, make_entry)
        self._bakes_table = bakes_table

    def _to_values(self, item: Attempt) -> dict[str, Any]:
        payload = asdict(item)
        return {
            "id": payload.pop("id"),
            "bake_id": payload.pop("bake_id"),
            "number": payload.pop("number"),
            "created_at": payload.pop("created_at"),
            "result": payload.pop("result"),
            "payload": payload,
        }

    def _from_record(self, record: Row) -> Attempt:
        return _attempt_from_record(record)

    async def after_insert(self, data: Attempt, conn: AsyncConnection) -> None:
        await self._sync_bake_status(data, conn)

    async def after_update(self, data: Attempt, conn: AsyncConnection) -> None:
        await self._sync_bake_status(data, conn)

    async def _sync_bake_status(self, data: Attempt, conn: AsyncConnection) -> None:
        max_number_query = (
            sa.select([sa.func.max(self._table.c.number)])
            .select_from(self._table)
            .where(self._table.c.bake_id == data.bake_id)
        )
        result = await self._fetchrow(max_number_query, conn)
        assert result is not None
        (max_number,) = result
        if data.number == max_number:
            query = (
                self._bakes_table.update()
                .values({"status": data.result})
                .where(self._bakes_table.c.id == data.bake_id)
            )
            async with self._engine.begin() as conn:
                try:
                    await self._execute(query, conn)
                except IntegrityError as exc:
                    if isinstance(exc.orig.__cause__, UniqueViolationError):
                        raise UniquenessError(
                            "There can be only one running bake with given name"
                        )
                    raise

    @trace
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
        bake_id: str | None = None,
    ) -> AsyncIterator[Attempt]:
        query = self._table.select()
        if bake_id is not None:
            query = query.where(self._table.c.bake_id == bake_id)
        async with self._engine.begin() as conn:
            async for record in await self._cursor(query, conn=conn):
                yield self._from_record(record)


class PostgresTaskStorage(TaskStorage, BasePostgresStorage[TaskData, Task]):
    def _to_values(self, item: Task) -> dict[str, Any]:
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

    def _from_record(self, record: Row) -> Task:
        payload = record["payload"]
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

    @trace
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
        attempt_id: str | None = None,
    ) -> AsyncIterator[Task]:
        query = self._table.select()
        if attempt_id is not None:
            query = query.where(self._table.c.attempt_id == attempt_id)
        async with self._engine.begin() as conn:
            async for record in await self._cursor(query, conn=conn):
                yield self._from_record(record)


class PostgresCacheEntryStorage(
    CacheEntryStorage, BasePostgresStorage[CacheEntryData, CacheEntry]
):
    def _to_values(self, item: CacheEntry) -> dict[str, Any]:
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

    def _from_record(self, record: Row) -> CacheEntry:
        payload = record["payload"]
        payload["id"] = record["id"]
        payload["project_id"] = record["project_id"]
        payload["task_id"] = _str2full_id(record["task_id"])
        payload["batch"] = record["batch"]
        payload["key"] = record["key"]
        payload["created_at"] = record["created_at"]
        return CacheEntry(**payload)

    @trace
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

    @trace
    async def delete_all(
        self,
        project_id: str | None = None,
        task_id: FullID | None = None,
        batch: str | None = None,
    ) -> None:
        query = self._table.delete()
        if project_id is not None:
            query = query.where(self._table.c.project_id == project_id)
        if task_id is not None:
            query = query.where(self._table.c.task_id == _full_id2str(task_id))
        if batch is not None:
            query = query.where(self._table.c.batch == batch)
        async with self._engine.begin() as conn:
            await self._execute(query, conn)


class PostgresConfigFileStorage(
    ConfigFileStorage, BasePostgresStorage[ConfigFileData, ConfigFile]
):
    def _to_values(self, item: ConfigFile) -> dict[str, Any]:
        payload = asdict(item)
        return {
            "id": payload.pop("id"),
            "bake_id": payload.pop("bake_id"),
            "filename": payload.pop("filename"),
            "content": payload.pop("content"),
            "payload": payload,
        }

    def _from_record(self, record: Row) -> ConfigFile:
        payload = record["payload"]
        payload["id"] = record["id"]
        payload["bake_id"] = record["bake_id"]
        payload["filename"] = record["filename"]
        payload["content"] = record["content"]
        return ConfigFile(**payload)


class PostgresBakeImageStorage(
    BakeImageStorage, BasePostgresStorage[BakeImageData, BakeImage]
):
    def _to_values(self, item: BakeImage) -> dict[str, Any]:
        payload = asdict(item)
        payload["yaml_defs"] = [
            _full_id2str(yaml_def) for yaml_def in payload["yaml_defs"]
        ]
        return {
            "id": payload.pop("id"),
            "bake_id": payload.pop("bake_id"),
            "ref": payload.pop("ref"),
            "status": payload.pop("status"),
            "payload": payload,
        }

    def _from_record(self, record: Row) -> BakeImage:
        payload = record["payload"]
        payload["id"] = record["id"]
        payload["bake_id"] = record["bake_id"]
        payload["ref"] = record["ref"]
        payload["status"] = ImageStatus(record["status"])
        if "yaml_defs" in payload:
            payload["yaml_defs"] = [
                _str2full_id(yaml_def) for yaml_def in payload["yaml_defs"]
            ]
        elif "prefix" in payload:
            # This is old entry
            payload["yaml_defs"] = [
                _str2full_id(payload.pop("prefix")) + (payload.pop("yaml_id"),)
            ]

        return BakeImage(**payload)

    @trace
    async def get_by_ref(self, bake_id: str, ref: str) -> BakeImage:
        query = (
            self._table.select()
            .where(self._table.c.bake_id == bake_id)
            .where(self._table.c.ref == ref)
        )
        record = await self._fetchrow(query)
        if not record:
            raise NotExistsError
        return self._from_record(record)

    async def list(
        self,
        bake_id: str | None = None,
    ) -> AsyncIterator[BakeImage]:
        query = self._table.select()
        if bake_id is not None:
            query = query.where(self._table.c.bake_id == bake_id)
        async with self._engine.begin() as conn:
            async for record in await self._cursor(query, conn=conn):
                yield self._from_record(record)


class PostgresStorage(Storage):
    projects: PostgresProjectStorage

    def __init__(self, engine: AsyncEngine) -> None:
        tables = FlowTables.create()
        self.projects = PostgresProjectStorage(
            table=tables.projects,
            engine=engine,
            id_prefix="projects",
            make_entry=Project.from_data_obj,
        )
        self.live_jobs = PostgresLiveJobsStorage(
            table=tables.live_jobs,
            engine=engine,
            id_prefix="live-job",
            make_entry=LiveJob.from_data_obj,
        )
        self.bakes = PostgresBakeStorage(
            bakes_table=tables.bakes,
            attempts_table=tables.attempts,
            engine=engine,
            id_prefix="bake",
            make_entry=Bake.from_data_obj,
        )
        self.attempts = PostgresAttemptStorage(
            attempts_table=tables.attempts,
            bakes_table=tables.bakes,
            engine=engine,
            id_prefix="attempt",
            make_entry=Attempt.from_data_obj,
        )
        self.tasks = PostgresTaskStorage(
            table=tables.tasks,
            engine=engine,
            id_prefix="task",
            make_entry=Task.from_data_obj,
        )
        self.cache_entries = PostgresCacheEntryStorage(
            table=tables.cache_entries,
            engine=engine,
            id_prefix="cache-entry",
            make_entry=CacheEntry.from_data_obj,
        )
        self.config_files = PostgresConfigFileStorage(
            table=tables.config_files,
            engine=engine,
            id_prefix="config-file",
            make_entry=ConfigFile.from_data_obj,
        )
        self.bake_images = PostgresBakeImageStorage(
            table=tables.bake_images,
            engine=engine,
            id_prefix="bake-image",
            make_entry=BakeImage.from_data_obj,
        )
