from typing import Any, Dict
from unittest.mock import ANY

from yarl import URL

from platform_neuro_flow_api.config import (
    Config,
    CORSConfig,
    PlatformAuthConfig,
    PostgresConfig,
    SentryConfig,
    ServerConfig,
)
from platform_neuro_flow_api.config_factory import EnvironConfigFactory


def test_create() -> None:
    environ: Dict[str, Any] = {
        "NP_NEURO_FLOW_API_HOST": "0.0.0.0",
        "NP_NEURO_FLOW_API_PORT": 8080,
        "NP_NEURO_FLOW_API_PLATFORM_AUTH_URL": "http://platformauthapi/api/v1",
        "NP_NEURO_FLOW_API_PLATFORM_AUTH_TOKEN": "platform-auth-token",
        "NP_SENTRY_URL": "https://test.com",
        "NP_SENTRY_CLUSTER": "test",
        "NP_CORS_ORIGINS": "https://domain1.com,http://do.main",
        "NP_NEURO_FLOW_API_ENABLE_DOCS": "true",
        "NP_DB_POSTGRES_DSN": "postgresql://postgres@localhost:5432/postgres",
        "NP_DB_POSTGRES_POOL_MIN": "50",
        "NP_DB_POSTGRES_POOL_MAX": "500",
    }
    config = EnvironConfigFactory(environ).create()
    assert config == Config(
        server=ServerConfig(host="0.0.0.0", port=8080),
        platform_auth=PlatformAuthConfig(
            url=URL("http://platformauthapi/api/v1"), token="platform-auth-token"
        ),
        cors=CORSConfig(["https://domain1.com", "http://do.main"]),
        sentry=SentryConfig(
            url="https://test.com",
            cluster="test",
        ),
        postgres=PostgresConfig(
            postgres_dsn="postgresql://postgres@localhost:5432/postgres",
            pool_min_size=50,
            pool_max_size=500,
            alembic=ANY,
        ),
        enable_docs=True,
    )
