from __future__ import annotations

import logging
import os
import pathlib
from collections.abc import Sequence

from yarl import URL

from alembic.config import Config as AlembicConfig

from .config import (
    Config,
    CORSConfig,
    PlatformApiConfig,
    PlatformAuthConfig,
    PostgresConfig,
    ServerConfig,
)

logger = logging.getLogger(__name__)


class EnvironConfigFactory:
    def __init__(self, environ: dict[str, str] | None = None) -> None:
        self._environ = environ or os.environ

    def create(self) -> Config:
        enable_docs = (
            self._environ.get("NP_NEURO_FLOW_API_ENABLE_DOCS", "false") == "true"
        )
        return Config(
            server=self._create_server(),
            platform_auth=self._create_platform_auth(),
            platform_api=self._create_platform_api(),
            cors=self.create_cors(),
            postgres=self.create_postgres(),
            enable_docs=enable_docs,
        )

    def _create_server(self) -> ServerConfig:
        host = self._environ.get("NP_NEURO_FLOW_API_HOST", ServerConfig.host)
        port = int(self._environ.get("NP_NEURO_FLOW_API_PORT", ServerConfig.port))
        return ServerConfig(host=host, port=port)

    def _create_platform_auth(self) -> PlatformAuthConfig:
        url = URL(self._environ["NP_NEURO_FLOW_API_PLATFORM_AUTH_URL"])
        token = self._environ["NP_NEURO_FLOW_API_PLATFORM_AUTH_TOKEN"]
        return PlatformAuthConfig(url=url, token=token)

    def _create_platform_api(self) -> PlatformApiConfig:
        url = URL(self._environ["NP_NEURO_FLOW_API_PLATFORM_API_URL"])
        token = self._environ["NP_NEURO_FLOW_API_PLATFORM_AUTH_TOKEN"]
        return PlatformApiConfig(url=url, token=token)

    def create_cors(self) -> CORSConfig:
        origins: Sequence[str] = CORSConfig.allowed_origins
        origins_str = self._environ.get("NP_CORS_ORIGINS", "").strip()
        if origins_str:
            origins = origins_str.split(",")
        return CORSConfig(allowed_origins=origins)

    def create_postgres(self) -> PostgresConfig:
        try:
            postgres_dsn = to_async_postgres_dsn(self._environ["NP_DB_POSTGRES_DSN"])
        except KeyError:
            # Temporary fix until postgres deployment is set
            postgres_dsn = ""
        pool_min_size = int(
            self._environ.get("NP_DB_POSTGRES_POOL_MIN", PostgresConfig.pool_min_size)
        )
        pool_max_size = int(
            self._environ.get("NP_DB_POSTGRES_POOL_MAX", PostgresConfig.pool_max_size)
        )
        connect_timeout_s = float(
            self._environ.get(
                "NP_DB_POSTGRES_CONNECT_TIMEOUT", PostgresConfig.connect_timeout_s
            )
        )
        command_timeout_s = PostgresConfig.command_timeout_s
        if self._environ.get("NP_DB_POSTGRES_COMMAND_TIMEOUT"):
            command_timeout_s = float(self._environ["NP_DB_POSTGRES_COMMAND_TIMEOUT"])
        return PostgresConfig(
            postgres_dsn=postgres_dsn,
            alembic=self.create_alembic(postgres_dsn),
            pool_min_size=pool_min_size,
            pool_max_size=pool_max_size,
            connect_timeout_s=connect_timeout_s,
            command_timeout_s=command_timeout_s,
        )

    def create_alembic(self, postgres_dsn: str) -> AlembicConfig:
        parent_path = pathlib.Path(__file__).resolve().parent.parent
        ini_path = str(parent_path / "alembic.ini")
        script_path = str(parent_path / "alembic")
        config = AlembicConfig(ini_path)
        config.set_main_option("script_location", script_path)
        postgres_dsn = to_sync_postgres_dsn(postgres_dsn)
        config.set_main_option("sqlalchemy.url", postgres_dsn.replace("%", "%%"))
        return config


syncpg_schema = "postgresql"
asyncpg_schema = "postgresql+asyncpg"


def to_async_postgres_dsn(dsn: str) -> str:
    if dsn.startswith(syncpg_schema + "://"):
        dsn = asyncpg_schema + dsn[len(syncpg_schema) :]
    return dsn


def to_sync_postgres_dsn(dsn: str) -> str:
    if dsn.startswith(asyncpg_schema + "://"):
        dsn = syncpg_schema + dsn[len(asyncpg_schema) :]
    return dsn
