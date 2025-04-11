from __future__ import annotations

from typing import Any
from unittest.mock import ANY

from yarl import URL

from platform_neuro_flow_api.config import (
    Config,
    CORSConfig,
    PlatformApiConfig,
    PlatformAuthConfig,
    PostgresConfig,
    SentryConfig,
    ServerConfig,
    ZipkinConfig,
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
        "NP_ZIPKIN_URL": "http://zipkin:9411",
        "NP_SENTRY_DSN": "https://test.com",
        "NP_SENTRY_CLUSTER_NAME": "test",
        "NP_CORS_ORIGINS": "https://domain1.com,http://do.main",
        "NP_NEURO_FLOW_API_ENABLE_DOCS": "true",
        "NP_DB_POSTGRES_DSN": dsn,
        "NP_DB_POSTGRES_POOL_MIN": "50",
        "NP_DB_POSTGRES_POOL_MAX": "500",
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
        cors=CORSConfig(["https://domain1.com", "http://do.main"]),
        zipkin=ZipkinConfig(url=URL("http://zipkin:9411")),
        sentry=SentryConfig(dsn=URL("https://test.com"), cluster_name="test"),
        postgres=PostgresConfig(
            postgres_dsn=dsn,
            pool_min_size=50,
            pool_max_size=500,
            alembic=ANY,
        ),
        enable_docs=True,
    )


def test_create_zipkin_none() -> None:
    result = EnvironConfigFactory({}).create_zipkin()

    assert result is None


def test_create_zipkin_default() -> None:
    env = {"NP_ZIPKIN_URL": "http://zipkin:9411"}
    result = EnvironConfigFactory(env).create_zipkin()

    assert result == ZipkinConfig(url=URL("http://zipkin:9411"))


def test_create_zipkin_custom() -> None:
    environ = {
        "NP_ZIPKIN_URL": "http://zipkin",
        "NP_ZIPKIN_APP_NAME": "config",
        "NP_ZIPKIN_SAMPLE_RATE": "1",
    }
    result = EnvironConfigFactory(environ).create_zipkin()

    assert result
    assert result.url == URL("http://zipkin")
    assert result.app_name == "config"
    assert result.sample_rate == 1


def test_create_sentry_none() -> None:
    result = EnvironConfigFactory({}).create_sentry()

    assert result is None


def test_create_sentry_default() -> None:
    env = {
        "NP_SENTRY_DSN": "http://sentry-dsn",
        "NP_SENTRY_CLUSTER_NAME": "test",
    }
    result = EnvironConfigFactory(env).create_sentry()

    assert result == SentryConfig(dsn=URL("http://sentry-dsn"), cluster_name="test")


def test_create_sentry_custom() -> None:
    environ = {
        "NP_SENTRY_DSN": "http://sentry-dsn",
        "NP_SENTRY_CLUSTER_NAME": "test",
        "NP_SENTRY_APP_NAME": "config",
        "NP_SENTRY_SAMPLE_RATE": "1.0",
    }
    result = EnvironConfigFactory(environ=environ).create_sentry()

    assert result == SentryConfig(
        dsn=URL("http://sentry-dsn"),
        cluster_name="test",
        app_name="config",
        sample_rate=1,
    )
