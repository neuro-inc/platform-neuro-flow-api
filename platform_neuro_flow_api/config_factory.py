import logging
import os
import pathlib
from collections.abc import Sequence
from typing import Optional

from yarl import URL

from alembic.config import Config as AlembicConfig

from .config import (
    Config,
    CORSConfig,
    PlatformAuthConfig,
    PostgresConfig,
    SentryConfig,
    ServerConfig,
    ZipkinConfig,
)

logger = logging.getLogger(__name__)


class EnvironConfigFactory:
    def __init__(self, environ: Optional[dict[str, str]] = None) -> None:
        self._environ = environ or os.environ

    def create(self) -> Config:
        enable_docs = (
            self._environ.get("NP_NEURO_FLOW_API_ENABLE_DOCS", "false") == "true"
        )
        return Config(
            server=self._create_server(),
            platform_auth=self._create_platform_auth(),
            cors=self.create_cors(),
            postgres=self.create_postgres(),
            zipkin=self.create_zipkin(),
            sentry=self.create_sentry(),
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

    def create_cors(self) -> CORSConfig:
        origins: Sequence[str] = CORSConfig.allowed_origins
        origins_str = self._environ.get("NP_CORS_ORIGINS", "").strip()
        if origins_str:
            origins = origins_str.split(",")
        return CORSConfig(allowed_origins=origins)

    def create_zipkin(self) -> Optional[ZipkinConfig]:
        if "NP_ZIPKIN_URL" not in self._environ:
            return None

        url = URL(self._environ["NP_ZIPKIN_URL"])
        app_name = self._environ.get("NP_ZIPKIN_APP_NAME", ZipkinConfig.app_name)
        sample_rate = float(
            self._environ.get("NP_ZIPKIN_SAMPLE_RATE", ZipkinConfig.sample_rate)
        )
        return ZipkinConfig(url=url, app_name=app_name, sample_rate=sample_rate)

    def create_sentry(self) -> Optional[SentryConfig]:
        if "NP_SENTRY_DSN" not in self._environ:
            return None

        return SentryConfig(
            dsn=URL(self._environ["NP_SENTRY_DSN"]),
            cluster_name=self._environ["NP_SENTRY_CLUSTER_NAME"],
            app_name=self._environ.get("NP_SENTRY_APP_NAME", SentryConfig.app_name),
            sample_rate=float(
                self._environ.get("NP_SENTRY_SAMPLE_RATE", SentryConfig.sample_rate)
            ),
        )

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
