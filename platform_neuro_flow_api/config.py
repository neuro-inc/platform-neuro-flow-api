from dataclasses import dataclass, field
from typing import Optional, Sequence

from yarl import URL


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
class SentryConfig:
    url: str
    cluster: str


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    platform_auth: PlatformAuthConfig
    cors: CORSConfig
    sentry: Optional[SentryConfig]
