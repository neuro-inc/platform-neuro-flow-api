from pathlib import Path
from typing import Any, Dict

import pytest
from yarl import URL

from platform_neuro_flow_api.config import (
    Config,
    CORSConfig,
    PlatformAuthConfig,
    SentryConfig,
    ServerConfig,
)
from platform_neuro_flow_api.config_factory import EnvironConfigFactory


CA_DATA_PEM = "this-is-certificate-authority-public-key"
TOKEN = "this-is-token"


@pytest.fixture()
def cert_authority_path(tmp_path: Path) -> str:
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(CA_DATA_PEM)
    return str(ca_path)


@pytest.fixture()
def token_path(tmp_path: Path) -> str:
    token_path = tmp_path / "token"
    token_path.write_text(TOKEN)
    return str(token_path)


def test_create(cert_authority_path: str, token_path: str) -> None:
    environ: Dict[str, Any] = {
        "NP_NEURO_FLOW_API_HOST": "0.0.0.0",
        "NP_NEURO_FLOW_API_PORT": 8080,
        "NP_NEURO_FLOW_API_PLATFORM_AUTH_URL": "http://platformauthapi/api/v1",
        "NP_NEURO_FLOW_API_PLATFORM_AUTH_TOKEN": "platform-auth-token",
        "NP_SENTRY_URL": "https://test.com",
        "NP_SENTRY_CLUSTER": "test",
        "NP_CORS_ORIGINS": "https://domain1.com,http://do.main",
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
    )
