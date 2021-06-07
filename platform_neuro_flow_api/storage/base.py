import datetime
import enum
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from typing import (
    AbstractSet,
    Any,
    AsyncIterator,
    Generic,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)


logger = logging.getLogger(__name__)


class StorageError(Exception):
    pass


class TransactionError(StorageError):
    pass


class NotExistsError(StorageError):
    pass


class ExistsError(StorageError):
    pass


class UniquenessError(StorageError):
    pass


FullID = Tuple[str, ...]


class TaskStatus(str, enum.Enum):
    UNKNOWN = "unknown"
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"
    CACHED = "cached"


class ImageStatus(str, enum.Enum):
    PENDING = "pending"
    BUILDING = "building"
    BUILT = "built"
    BUILD_FAILED = "build_failed"


_C = TypeVar("_C", bound="HasId")


@dataclass(frozen=True)
class HasId:
    id: str  # Unique, generated by storage

    @classmethod
    def from_data_obj(cls: Type[_C], id: str, data_obj: Any) -> _C:
        assert issubclass(cls, type(data_obj))

        return cls(  # type: ignore
            id=id,
            **{field.name: getattr(data_obj, field.name) for field in fields(data_obj)},
        )


@dataclass(frozen=True)
class ProjectData:
    name: str
    owner: str
    cluster: str


@dataclass(frozen=True)
class Project(HasId, ProjectData):
    pass


@dataclass(frozen=True)
class LiveJobData:
    yaml_id: str
    project_id: str
    multi: bool
    tags: Sequence[str]


@dataclass(frozen=True)
class LiveJob(HasId, LiveJobData):
    pass


@dataclass(frozen=True)
class ConfigFileData:
    bake_id: str
    filename: str
    content: str


@dataclass(frozen=True)
class ConfigFile(HasId, ConfigFileData):
    pass


@dataclass(frozen=True)
class ConfigsMeta:
    workspace: str
    flow_config_id: str
    project_config_id: Optional[str]
    action_config_ids: Mapping[str, str]


@dataclass(frozen=True)
class AttemptData:
    bake_id: str
    number: int
    created_at: datetime.datetime
    result: TaskStatus
    configs_meta: ConfigsMeta
    executor_id: Optional[str] = None


@dataclass(frozen=True)
class Attempt(AttemptData, HasId):
    pass


@dataclass(frozen=True)
class TaskStatusItem:
    when: datetime.datetime
    status: TaskStatus


@dataclass(frozen=True)
class TaskData:
    yaml_id: FullID
    attempt_id: str
    raw_id: Optional[str]
    outputs: Optional[Mapping[str, str]]
    state: Optional[Mapping[str, str]]
    statuses: Sequence[TaskStatusItem]

    def __post_init__(self) -> None:
        finished = any(
            entry.status in {TaskStatus.SUCCEEDED, TaskStatus.CACHED}
            for entry in self.statuses
        )
        if finished:
            assert self.outputs is not None
            assert self.state is not None


@dataclass(frozen=True)
class Task(HasId, TaskData):
    pass


@dataclass(frozen=True)
class BakeData:
    project_id: str
    batch: str
    created_at: datetime.datetime
    # prefix -> { id -> deps }
    graphs: Mapping[FullID, Mapping[FullID, AbstractSet[FullID]]]
    params: Optional[Mapping[str, str]] = None
    name: Optional[str] = None
    tags: Sequence[str] = ()
    last_attempt: Optional[Attempt] = None

    def __post_init__(self) -> None:
        # Ensure that tags is a tuple for correct __eq__
        object.__setattr__(self, "tags", tuple(self.tags))


@dataclass(frozen=True)
class Bake(BakeData, HasId):
    pass


@dataclass(frozen=True)
class CacheEntryData:
    project_id: str
    task_id: FullID
    batch: str
    key: str
    created_at: datetime.datetime
    outputs: Mapping[str, str]
    state: Mapping[str, str]
    raw_id: str = ""


@dataclass(frozen=True)
class CacheEntry(CacheEntryData, HasId):
    pass


@dataclass(frozen=True)
class BakeImageData:
    bake_id: str
    yaml_defs: Sequence[FullID]
    ref: str
    status: ImageStatus
    context_on_storage: Optional[str] = None
    dockerfile_rel: Optional[str] = None
    builder_job_id: Optional[str] = None

    @property
    def prefix(self) -> FullID:
        *prefix, _ = self.yaml_defs[0]
        return tuple(prefix)

    @property
    def yaml_id(self) -> str:
        *_, yaml_id = self.yaml_defs[0]
        return yaml_id


@dataclass(frozen=True)
class BakeImage(BakeImageData, HasId):
    pass


_D = TypeVar("_D")
_E = TypeVar("_E", bound=HasId)


class BaseStorage(ABC, Generic[_D, _E]):
    @abstractmethod
    async def insert(self, data: _E) -> None:
        pass

    @abstractmethod
    async def create(self, data: _D) -> _E:
        pass

    @abstractmethod
    async def update(self, data: _E) -> None:
        pass

    @abstractmethod
    async def get(self, id: str) -> _E:
        pass


class ProjectStorage(BaseStorage[ProjectData, Project], ABC):
    @abstractmethod
    async def get_by_name(
        self,
        name: str,
        owner: str,
        cluster: str,
    ) -> Project:
        pass

    @abstractmethod
    def list(
        self,
        name: Optional[str] = None,
        owner: Optional[str] = None,
        cluster: Optional[str] = None,
    ) -> AsyncIterator[Project]:
        pass


class LiveJobStorage(BaseStorage[LiveJobData, LiveJob], ABC):
    @abstractmethod
    async def get_by_yaml_id(self, yaml_id: str, project_id: str) -> LiveJob:
        pass

    @abstractmethod
    async def update_or_create(self, data: LiveJobData) -> LiveJob:
        pass

    @abstractmethod
    def list(self, project_id: Optional[str] = None) -> AsyncIterator[LiveJob]:
        pass


class BakeStorage(BaseStorage[BakeData, Bake], ABC):
    @abstractmethod
    def list(
        self,
        project_id: Optional[str] = None,
        name: Optional[str] = None,
        tags: AbstractSet[str] = frozenset(),
        *,
        reverse: bool = False,
        since: Optional[datetime.datetime] = None,
        until: Optional[datetime.datetime] = None,
        fetch_last_attempt: bool = False,
    ) -> AsyncIterator[Bake]:
        pass

    @abstractmethod
    async def get(self, id: str, *, fetch_last_attempt: bool = False) -> Bake:
        pass

    @abstractmethod
    async def get_by_name(
        self,
        project_id: str,
        name: str,
        *,
        fetch_last_attempt: bool = False,
    ) -> Bake:
        pass


class AttemptStorage(BaseStorage[AttemptData, Attempt], ABC):
    @abstractmethod
    async def get_by_number(self, bake_id: str, number: int) -> Attempt:
        pass

    @abstractmethod
    def list(self, bake_id: Optional[str] = None) -> AsyncIterator[Attempt]:
        pass


class TaskStorage(BaseStorage[TaskData, Task], ABC):
    @abstractmethod
    async def get_by_yaml_id(
        self,
        yaml_id: FullID,
        attempt_id: str,
    ) -> Task:
        pass

    @abstractmethod
    def list(self, attempt_id: Optional[str] = None) -> AsyncIterator[Task]:
        pass


class CacheEntryStorage(BaseStorage[CacheEntryData, CacheEntry], ABC):
    @abstractmethod
    async def get_by_key(
        self, project_id: str, task_id: FullID, batch: str, key: str
    ) -> CacheEntry:
        pass

    @abstractmethod
    async def delete_all(
        self,
        project_id: Optional[str] = None,
        task_id: Optional[FullID] = None,
        batch: Optional[str] = None,
    ) -> None:
        pass


class BakeImageStorage(BaseStorage[BakeImageData, BakeImage], ABC):
    @abstractmethod
    async def get_by_ref(self, bake_id: str, ref: str) -> BakeImage:
        pass

    @abstractmethod
    def list(self, bake_id: Optional[str] = None) -> AsyncIterator[BakeImage]:
        pass


class ConfigFileStorage(BaseStorage[ConfigFileData, ConfigFile], ABC):
    pass


class Storage(ABC):
    projects: ProjectStorage
    live_jobs: LiveJobStorage
    bakes: BakeStorage
    attempts: AttemptStorage
    tasks: TaskStorage
    cache_entries: CacheEntryStorage
    config_files: ConfigFileStorage
    bake_images: BakeImageStorage
