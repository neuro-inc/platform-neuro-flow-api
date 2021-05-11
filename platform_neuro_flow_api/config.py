from dataclasses import dataclass, field
from typing import Optional, Sequence

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
class CORSConfig:
    allowed_origins: Sequence[str] = ()


@dataclass(frozen=True)
class ZipkinConfig:
    url: URL
    app_name: str = "platform-neuro-flow-api"
    sample_rate: float = 0.0


@dataclass(frozen=True)
class SentryConfig:
    dsn: URL
    cluster_name: str
    app_name: str = "platform-neuro-flow-api"
    sample_rate: float = 0.0


@dataclass(frozen=True)
class PostgresConfig:
    postgres_dsn: str

    alembic: AlembicConfig

    # based on defaults
    # https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.connect
    pool_min_size: int = 10
    pool_max_size: int = 10

    connect_timeout_s: float = 60.0
    command_timeout_s: Optional[float] = 60.0


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    platform_auth: PlatformAuthConfig
    cors: CORSConfig
    postgres: PostgresConfig
    enable_docs: bool = False
    zipkin: Optional[ZipkinConfig] = None
    sentry: Optional[SentryConfig] = None
