from __future__ import annotations

import logging
import os
import pathlib

from apolo_events_client import EventsClientConfig
from apolo_kube_client import KubeClientAuthType, KubeConfig
from yarl import URL

from alembic.config import Config as AlembicConfig

from .config import (
    Config,
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
            kube=self.create_kube(),
            postgres=self.create_postgres(),
            events=self.create_events(),
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
        pool_recycle = int(
            self._environ.get(
                "NP_DB_POSTGRES_POOL_RECYCLE", PostgresConfig.pool_recycle
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
            pool_recycle=pool_recycle,
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

    def create_events(self) -> EventsClientConfig | None:
        if "NP_NEURO_FLOW_API_PLATFORM_EVENTS_URL" in self._environ:
            url = URL(self._environ["NP_NEURO_FLOW_API_PLATFORM_EVENTS_URL"])
            token = self._environ["NP_NEURO_FLOW_API_PLATFORM_EVENTS_TOKEN"]
            return EventsClientConfig(url=url, token=token, name="platform-neuro-flow")
        return None

    def create_kube(self) -> KubeConfig:
        endpoint_url = self._environ["NP_NEURO_FLOW_API_K8S_API_URL"]
        auth_type = KubeClientAuthType(
            self._environ.get(
                "NP_NEURO_FLOW_API_K8S_AUTH_TYPE", KubeClientAuthType.NONE
            )
        )
        ca_path = self._environ.get("NP_NEURO_FLOW_API_K8S_CA_PATH")
        ca_data = pathlib.Path(ca_path).read_text() if ca_path else None

        token_path = self._environ.get("NP_NEURO_FLOW_API_K8S_TOKEN_PATH")
        token = pathlib.Path(token_path).read_text() if token_path else None

        return KubeConfig(
            endpoint_url=endpoint_url,
            cert_authority_data_pem=ca_data,
            auth_type=auth_type,
            auth_cert_path=self._environ.get("NP_NEURO_FLOW_API_K8S_AUTH_CERT_PATH"),
            auth_cert_key_path=self._environ.get(
                "NP_NEURO_FLOW_API_K8S_AUTH_CERT_KEY_PATH"
            ),
            token=token,
            token_path=token_path,
            client_conn_timeout_s=int(
                self._environ.get("NP_SECRETS_K8S_CLIENT_CONN_TIMEOUT")
                or KubeConfig.model_fields["client_conn_timeout_s"].default
            ),
            client_read_timeout_s=int(
                self._environ.get("NP_SECRETS_K8S_CLIENT_READ_TIMEOUT")
                or KubeConfig.model_fields["client_read_timeout_s"].default
            ),
            client_conn_pool_size=int(
                self._environ.get("NP_SECRETS_K8S_CLIENT_CONN_POOL_SIZE")
                or KubeConfig.model_fields["client_conn_pool_size"].default
            ),
        )


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
