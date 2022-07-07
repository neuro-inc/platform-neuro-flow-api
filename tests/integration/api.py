from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import aiohttp.web
import pytest
from neuro_sdk import JobDescription
from yarl import URL

from platform_neuro_flow_api.config import PlatformApiConfig

from tests.integration.conftest import ApiAddress, create_local_app_server


@dataclass()
class PlatformApiServer:
    address: ApiAddress | None = None
    jobs: list[JobDescription] = field(default_factory=list)

    @property
    def url(self) -> URL:
        assert self.address
        return URL(f"http://{self.address.host}:{self.address.port}/api/v1/")

    def _serialize_job(self, job: JobDescription) -> dict[str, Any]:
        return {
            "id": job.id,
            "owner": job.owner,
            "cluster_name": job.cluster_name,
            "org_name": job.org_name,
            "status": job.status.value,
            "history": {
                "status": job.status.value,
                "reason": job.history.reason,
                "description": job.history.description,
                "created_at": (
                    job.history.created_at.isoformat()
                    if job.history.created_at is not None
                    else None
                ),
                "started_at": (
                    job.history.started_at.isoformat()
                    if job.history.started_at is not None
                    else None
                ),
                "finished_at": (
                    job.history.finished_at.isoformat()
                    if job.history.finished_at is not None
                    else None
                ),
                "exit_code": job.history.exit_code,
            },
            "container": {
                "image": job.container.image.as_docker_url(),
                "resources": {
                    "cpu": job.container.resources.cpu,
                    "gpu": job.container.resources.gpu,
                    "gpu_model": job.container.resources.gpu_model,
                    "memory": job.container.resources.memory,
                    "extshm": job.container.resources.shm,
                },
            },
            "scheduler_enabled": job.scheduler_enabled,
            "pass_config": job.pass_config,
            "http_url": str(job.http_url),
            "uri": str(job.uri),
            "total_price_credits": str(job.total_price_credits),
            "price_credits_per_hour": str(job.price_credits_per_hour),
        }

    async def handle_jobs(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.json_response(
            [self._serialize_job(job) for job in self.jobs]
        )

    async def handle_get_job(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        job_id = request.match_info["job"]
        for job in self.jobs:
            if job.id == job_id:
                return aiohttp.web.json_response(self._serialize_job(job))
        raise aiohttp.web.HTTPNotFound

    async def handle_config(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        data = {
            "authorized": True,
            "admin_url": "https://example.org",
            "auth_url": "https://example.org",
            "token_url": "https://example.org",
            "logout_url": "https://example.org",
            "client_id": "test",
            "audience": "world",
            "callback_urls": ["https://example.org"],
            "headless_callback_url": "https://example.org",
            "resource_presets": [{"name": "default", "cpu": 0.1, "memory": 1024**3}],
            "clusters": [
                {
                    "name": "default",
                    "registry_url": "https://registry-dev.test.com",
                    "storage_url": "https://storage-dev.test.com",
                    "buckets_url": "https://buckets-dev.test.com",
                    "users_url": "https://users-dev.test.com",
                    "monitoring_url": "https://monitoring-dev.test.com",
                    "secrets_url": "https://secrets-dev.test.com",
                    "disks_url": "https://disks-dev.test.com",
                    "resource_presets": [
                        {
                            "name": "gpu-small",
                            "cpu": 7,
                            "memory": 30 * 1024**2,
                            "gpu": 1,
                            "gpu_model": "nvidia-tesla-k80",
                        },
                        {
                            "name": "gpu-large",
                            "cpu": 7,
                            "memory": 60 * 1024**2,
                            "gpu": 1,
                            "gpu_model": "nvidia-tesla-v100",
                        },
                        {"name": "cpu-small", "cpu": 2, "memory": 2 * 1024**3},
                        {"name": "cpu-large", "cpu": 3, "memory": 14 * 1024**3},
                    ],
                }
            ],
        }
        return aiohttp.web.json_response(data)


@pytest.fixture
async def mock_platform_api_server() -> AsyncIterator[PlatformApiServer]:
    api_server = PlatformApiServer()

    def _create_app() -> aiohttp.web.Application:
        app = aiohttp.web.Application()
        app.router.add_routes(
            (
                aiohttp.web.get(
                    "/api/v1/jobs",
                    api_server.handle_jobs,
                ),
                aiohttp.web.get(
                    "/api/v1/jobs/{job}",
                    api_server.handle_get_job,
                ),
                aiohttp.web.get(
                    "/api/v1/config",
                    api_server.handle_config,
                ),
            )
        )
        return app

    app = _create_app()
    async with create_local_app_server(app, port=8085) as ip_address:
        api_server.address = ip_address
        yield api_server


@pytest.fixture
def platform_api_url(
    mock_platform_api_server: PlatformApiServer,
) -> URL:
    return mock_platform_api_server.url


@pytest.fixture
def platform_api_config(platform_api_url: URL) -> PlatformApiConfig:
    return PlatformApiConfig(url=platform_api_url, token="anything")
