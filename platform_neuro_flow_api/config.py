from __future__ import annotations

from dataclasses import dataclass, field

from apolo_events_client import EventsClientConfig
from apolo_kube_client import KubeConfig
from yarl import URL

from alembic.config import Config as AlembicConfig


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080


@dataclass(frozen=True)
class PlatformAuthConfig:
    url: URL
    token: str = field(repr=False)


@dataclass(frozen=True)
class PlatformApiConfig:
    url: URL
    token: str = field(repr=False)


@dataclass(frozen=True)
class PostgresConfig:
    postgres_dsn: str

    alembic: AlembicConfig

    # based on defaults
    # https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.connect
    pool_min_size: int = 10
    pool_max_size: int = 10

    connect_timeout_s: float = 60.0
    command_timeout_s: float | None = 60.0

    pool_recycle: int = 3_600  # 1 hour


@dataclass(frozen=True)
class WatchersConfig:
    polling_interval_sec: int = 60


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    platform_auth: PlatformAuthConfig
    platform_api: PlatformApiConfig
    postgres: PostgresConfig
    kube: KubeConfig | None = None
    watchers: WatchersConfig = WatchersConfig()
    events: EventsClientConfig | None = None
    enable_docs: bool = False
