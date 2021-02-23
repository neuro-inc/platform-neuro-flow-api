import datetime
import enum
import logging
from abc import ABC, abstractmethod
from typing import AbstractSet, AsyncIterator, Mapping, Optional, Sequence, Tuple

from attr import dataclass


logger = logging.getLogger(__name__)


class JobsStorageException(Exception):
    pass


class JobStorageTransactionError(JobsStorageException):
    pass


@dataclass(frozen=True)
class Project:
    id: str
    owner: str


@dataclass(frozen=True)
class LiveJob:
    id: str
    project_id: str
    multi: bool
    tags: Sequence[str]


FullID = Tuple[str, ...]


@dataclass(frozen=True)
class Bake:
    id: str
    project_id: str
    batch: str
    when: datetime.datetime
    suffix: str
    # prefix -> { id -> deps }
    graphs: Mapping[FullID, Mapping[FullID, AbstractSet[FullID]]]
    params: Optional[Mapping[str, str]]


class TaskStatus(str, enum.Enum):
    UNKNOWN = "unknown"
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"
    CACHED = "cached"


@dataclass(frozen=True)
class Attempt:
    bake_id: str
    number: int
    when: datetime.datetime
    result: TaskStatus


@dataclass(frozen=True)
class TaskStatusItem:
    when: datetime.datetime
    status: TaskStatus


@dataclass(frozen=True)
class Task:
    id: FullID
    bake_id: str
    attempt: int
    raw_id: Optional[str]
    outputs: Optional[Mapping[str, str]]
    state: Optional[Mapping[str, str]]
    statuses: Sequence[TaskStatusItem]


class ProjectStorage(ABC):
    @dataclass(frozen=True)
    class Filter:
        owner: Optional[str]

    @abstractmethod
    async def create(self, project: Project) -> None:
        pass

    @abstractmethod
    async def get(self, id: str) -> Project:
        pass

    @abstractmethod
    async def list(self, filter: "ProjectStorage.Filter") -> AsyncIterator[Project]:
        pass


class LiveJobStorage(ABC):
    @dataclass(frozen=True)
    class Filter:
        project_id: Optional[str]

    @abstractmethod
    async def update_or_create(self, live_job: LiveJob) -> None:
        pass

    @abstractmethod
    async def get(self, id: str) -> LiveJob:
        pass

    @abstractmethod
    async def list(self, filter: "LiveJobStorage.Filter") -> AsyncIterator[LiveJob]:
        pass


class BakeStorage(ABC):
    @dataclass(frozen=True)
    class Filter:
        project_id: Optional[str]

    @abstractmethod
    async def create(self, bake: Bake) -> None:
        pass

    @abstractmethod
    async def get(self, id: str) -> Bake:
        pass

    @abstractmethod
    async def list(self, filter: "BakeStorage.Filter") -> AsyncIterator[Bake]:
        pass


class AttemptStorage(ABC):
    @dataclass(frozen=True)
    class Filter:
        bake_id: Optional[str]

    @abstractmethod
    async def create(self, live_job: Attempt) -> None:
        pass

    @abstractmethod
    async def get(self, id: str) -> Attempt:
        pass

    @abstractmethod
    async def list(self, filter: "AttemptStorage.Filter") -> AsyncIterator[Attempt]:
        pass


class TaskStorage(ABC):
    @dataclass(frozen=True)
    class Filter:
        bake_id: Optional[str]
        attempt: int

    @abstractmethod
    async def create(self, live_job: Task) -> None:
        pass

    @abstractmethod
    async def update(self, live_job: Task) -> None:
        pass

    @abstractmethod
    async def get(self, id: str) -> Attempt:
        pass

    @abstractmethod
    async def list(self, filter: "AttemptStorage.Filter") -> AsyncIterator[Attempt]:
        pass


class Storage(ABC):

    projects: ProjectStorage
    live_jobs: LiveJobStorage
    bakes: BakeStorage
    attempts: AttemptStorage
    tasks: TaskStorage
