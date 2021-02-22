import logging
import os
from typing import Dict, Optional, Sequence

from yarl import URL

from .config import Config, CORSConfig, PlatformAuthConfig, SentryConfig, ServerConfig


logger = logging.getLogger(__name__)


class EnvironConfigFactory:
    def __init__(self, environ: Optional[Dict[str, str]] = None) -> None:
        self._environ = environ or os.environ

    def create(self) -> Config:
        enable_docs = (
            self._environ.get("NP_NEURO_FLOW_API_ENABLE_DOCS", "false") == "true"
        )
        return Config(
            server=self._create_server(),
            platform_auth=self._create_platform_auth(),
            cors=self.create_cors(),
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

    def create_sentry(self) -> Optional[SentryConfig]:
        sentry_url = self._environ.get("NP_SENTRY_URL")
        sentry_cluster = self._environ.get("NP_SENTRY_CLUSTER")
        if sentry_url and sentry_cluster:
            return SentryConfig(sentry_url, sentry_cluster)
        return None
