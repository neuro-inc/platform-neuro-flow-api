from __future__ import annotations

from typing import Any
from unittest.mock import ANY

from apolo_kube_client import KubeClientAuthType, KubeConfig
from yarl import URL

from platform_neuro_flow_api import APP_NAME
from platform_neuro_flow_api.config import (
    Config,
    PlatformApiConfig,
    PlatformAuthConfig,
    PostgresConfig,
    ServerConfig,
)
from platform_neuro_flow_api.config_factory import EnvironConfigFactory


def test_create() -> None:
    dsn = "postgresql+asyncpg://postgres@localhost:5432/postgres"
    environ: dict[str, Any] = {
        "NP_NEURO_FLOW_API_HOST": "0.0.0.0",
        "NP_NEURO_FLOW_API_PORT": 8080,
        "NP_NEURO_FLOW_API_PLATFORM_AUTH_URL": "http://platformauthapi/api/v1",
        "NP_NEURO_FLOW_API_PLATFORM_API_URL": "http://platformapi",
        "NP_NEURO_FLOW_API_PLATFORM_AUTH_TOKEN": "platform-auth-token",
        "NP_NEURO_FLOW_API_K8S_API_URL": "https://localhost:8443",
        "SENTRY_DSN": "https://test.com",
        "SENTRY_CLUSTER_NAME": "test",
        "SENTRY_APP_NAME": f"{APP_NAME}-test",
        "NP_NEURO_FLOW_API_ENABLE_DOCS": "true",
        "NP_DB_POSTGRES_DSN": dsn,
        "NP_DB_POSTGRES_POOL_MIN": "50",
        "NP_DB_POSTGRES_POOL_MAX": "500",
        "NP_DB_POSTGRES_POOL_RECYCLE": "7200",
    }
    config = EnvironConfigFactory(environ).create()
    assert config == Config(
        server=ServerConfig(host="0.0.0.0", port=8080),
        platform_auth=PlatformAuthConfig(
            url=URL("http://platformauthapi/api/v1"), token="platform-auth-token"
        ),
        platform_api=PlatformApiConfig(
            url=URL("http://platformapi"), token="platform-auth-token"
        ),
        postgres=PostgresConfig(
            postgres_dsn=dsn,
            pool_min_size=50,
            pool_max_size=500,
            alembic=ANY,
            pool_recycle=7_200,
        ),
        kube=KubeConfig(
            endpoint_url="https://localhost:8443", auth_type=KubeClientAuthType.NONE
        ),
        enable_docs=True,
    )
