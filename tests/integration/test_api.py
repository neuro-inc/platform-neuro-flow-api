from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Protocol

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from aiohttp.web_exceptions import (
    HTTPConflict,
    HTTPCreated,
    HTTPForbidden,
    HTTPNoContent,
    HTTPNotFound,
    HTTPUnauthorized,
)
from apolo_api_client import JobStatus

from platform_neuro_flow_api.api import create_app
from platform_neuro_flow_api.config import Config
from platform_neuro_flow_api.storage.base import Attempt, Bake, Project

from ..utils import make_job
from .api import PlatformApiServer
from .auth import ProjectGranter, UserFactory, _User
from .conftest import ApiAddress, create_local_app_server


@dataclass(frozen=True)
class NeuroFlowApiEndpoints:
    address: ApiAddress

    @property
    def server_base_url(self) -> str:
        return f"http://{self.address.host}:{self.address.port}"

    @property
    def api_v1_endpoint(self) -> str:
        return f"{self.server_base_url}/api/v1"

    @property
    def ping_url(self) -> str:
        return f"{self.api_v1_endpoint}/ping"

    @property
    def secured_ping_url(self) -> str:
        return f"{self.api_v1_endpoint}/secured-ping"

    @property
    def openapi_json_url(self) -> str:
        return f"{self.server_base_url}/api/docs/v1/flow/ui/swagger.json"

    @property
    def projects_url(self) -> str:
        return f"{self.server_base_url}/api/v1/flow/projects"

    def project_url(self, id: str) -> str:
        return f"{self.projects_url}/{id}"

    @property
    def project_by_name_url(self) -> str:
        return f"{self.projects_url}/by_name"

    @property
    def live_jobs_url(self) -> str:
        return f"{self.server_base_url}/api/v1/flow/live_jobs"

    def live_job_url(self, id: str) -> str:
        return f"{self.live_jobs_url}/{id}"

    @property
    def live_job_by_yaml_id_url(self) -> str:
        return f"{self.live_jobs_url}/by_yaml_id"

    @property
    def live_job_replace_url(self) -> str:
        return f"{self.live_jobs_url}/replace"

    @property
    def bakes_url(self) -> str:
        return f"{self.server_base_url}/api/v1/flow/bakes"

    def bake_url(self, id: str) -> str:
        return f"{self.bakes_url}/{id}"

    @property
    def bake_by_name_url(self) -> str:
        return f"{self.bakes_url}/by_name"

    @property
    def attempts_url(self) -> str:
        return f"{self.server_base_url}/api/v1/flow/attempts"

    def attempt_url(self, id: str) -> str:
        return f"{self.attempts_url}/{id}"

    @property
    def attempt_replace_url(self) -> str:
        return f"{self.attempts_url}/replace"

    @property
    def attempt_by_number_url(self) -> str:
        return f"{self.attempts_url}/by_number"

    @property
    def tasks_url(self) -> str:
        return f"{self.server_base_url}/api/v1/flow/tasks"

    def task_url(self, id: str) -> str:
        return f"{self.tasks_url}/{id}"

    @property
    def task_replace_url(self) -> str:
        return f"{self.tasks_url}/replace"

    @property
    def task_by_yaml_id_url(self) -> str:
        return f"{self.tasks_url}/by_yaml_id"

    @property
    def cache_entries_url(self) -> str:
        return f"{self.server_base_url}/api/v1/flow/cache_entries"

    def cache_entry_url(self, id: str) -> str:
        return f"{self.cache_entries_url}/{id}"

    @property
    def cache_entry_by_key_url(self) -> str:
        return f"{self.cache_entries_url}/by_key"

    @property
    def config_files_url(self) -> str:
        return f"{self.server_base_url}/api/v1/flow/config_files"

    def config_file_url(self, id: str) -> str:
        return f"{self.config_files_url}/{id}"

    @property
    def bake_images_url(self) -> str:
        return f"{self.server_base_url}/api/v1/flow/bake_images"

    def bake_image_url(self, id: str) -> str:
        return f"{self.bake_images_url}/{id}"

    @property
    def bake_image_replace_url(self) -> str:
        return f"{self.bake_images_url}/replace"

    @property
    def bake_image_by_ref(self) -> str:
        return f"{self.bake_images_url}/by_ref"


@pytest.fixture
async def neuro_flow_api(config: Config) -> AsyncIterator[NeuroFlowApiEndpoints]:
    app = await create_app(config)
    async with create_local_app_server(app, port=8080) as address:
        yield NeuroFlowApiEndpoints(address=address)


class TestApi:
    async def test_doc_available_when_enabled(
        self, config: Config, client: aiohttp.ClientSession
    ) -> None:
        config = replace(config, enable_docs=True)
        app = await create_app(config)
        async with create_local_app_server(app, port=8080) as address:
            endpoints = NeuroFlowApiEndpoints(address=address)
            async with client.get(endpoints.openapi_json_url) as resp:
                assert resp.status == HTTPOk.status_code
                assert await resp.json()

    async def test_no_docs_when_disabled(
        self, config: Config, client: aiohttp.ClientSession
    ) -> None:
        config = replace(config, enable_docs=False)
        app = await create_app(config)
        async with create_local_app_server(app, port=8080) as address:
            endpoints = NeuroFlowApiEndpoints(address=address)
            async with client.get(endpoints.openapi_json_url) as resp:
                assert resp.status == HTTPNotFound.status_code

    async def test_ping(
        self, neuro_flow_api: NeuroFlowApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(neuro_flow_api.ping_url) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Pong"

    async def test_secured_ping(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        client: aiohttp.ClientSession,
        admin_token: str,
    ) -> None:
        headers = {"Authorization": f"Bearer {admin_token}"}
        async with client.get(neuro_flow_api.secured_ping_url, headers=headers) as resp:
            assert resp.status == HTTPOk.status_code
            text = await resp.text()
            assert text == "Secured Pong"

    async def test_secured_ping_no_token_provided_unauthorized(
        self, neuro_flow_api: NeuroFlowApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        url = neuro_flow_api.secured_ping_url
        async with client.get(url) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    async def test_secured_ping_non_existing_token_unauthorized(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        client: aiohttp.ClientSession,
        token_factory: Callable[[str], str],
    ) -> None:
        url = neuro_flow_api.secured_ping_url
        token = token_factory("non-existing-user")
        headers = {"Authorization": f"Bearer {token}"}
        async with client.get(url, headers=headers) as resp:
            assert resp.status == HTTPUnauthorized.status_code

    async def test_ping_unknown_origin(
        self, neuro_flow_api: NeuroFlowApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(
            neuro_flow_api.ping_url, headers={"Origin": "http://unknown"}
        ) as response:
            assert response.status == HTTPOk.status_code, await response.text()
            assert "Access-Control-Allow-Origin" not in response.headers


class TestProjectsApi:
    async def test_projects_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["name"] == "test"
            assert payload["owner"] == regular_user.name
            assert payload["cluster"] == "test-cluster"
            assert payload["project_name"] == "test-project"
            assert "id" in payload

    async def test_projects_create_with_default_project(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": regular_user.name,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["name"] == "test"
            assert payload["owner"] == regular_user.name
            assert payload["cluster"] == "test-cluster"
            assert payload["project_name"] == regular_user.name
            assert "id" in payload

    async def test_projects_with_org_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_org_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "org_name": "test-org",
                "project_name": "test-project",
            },
            headers=regular_org_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["name"] == "test"
            assert payload["owner"] == regular_org_user.name
            assert payload["cluster"] == "test-cluster"
            assert payload["org_name"] == "test-org"
            assert payload["project_name"] == "test-project"
            assert "id" in payload

    async def test_projects_get_by_id(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            project_id = (await resp.json())["id"]
        async with client.get(
            url=neuro_flow_api.project_url(project_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["name"] == "test"
            assert payload["owner"] == regular_user.name
            assert payload["cluster"] == "test-cluster"
            assert payload["project_name"] == "test-project"
            assert "id" in payload

    async def test_projects_get_by_name(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            project_id = (await resp.json())["id"]
        async with client.get(
            url=neuro_flow_api.project_by_name_url,
            params={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["name"] == "test"
            assert payload["owner"] == regular_user.name
            assert payload["cluster"] == "test-cluster"
            assert payload["project_name"] == "test-project"
            assert payload["id"] == project_id

    async def test_projects_get_by_name_with_org(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_org_user: _User,
        client: aiohttp.ClientSession,
        org_name: str,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "org_name": org_name,
                "project_name": regular_org_user.name,
            },
            headers=regular_org_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            project_id = (await resp.json())["id"]
        async with client.get(
            url=neuro_flow_api.project_by_name_url,
            params={"name": "test", "cluster": "test-cluster", "org_name": org_name},
            headers=regular_org_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["name"] == "test"
            assert payload["owner"] == regular_org_user.name
            assert payload["cluster"] == "test-cluster"
            assert payload["org_name"] == "test-org"
            assert payload["id"] == project_id

    async def test_shared_projects_get_by_name(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: UserFactory,
        client: aiohttp.ClientSession,
        grant_project_permission: ProjectGranter,
    ) -> None:
        user1 = await regular_user_factory(project_name="test-project1")
        user2 = await regular_user_factory(project_name="test-project2")
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project1",
            },
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            data = await resp.json()
            project = Project(
                id=data["id"],
                cluster=data["cluster"],
                name=data["name"],
                org_name=data["org_name"],
                owner=data["owner"],
                project_name=data["project_name"],
            )
        await grant_project_permission(user2, project)
        async with client.get(
            url=neuro_flow_api.project_by_name_url,
            params={
                "name": project.name,
                "cluster": project.cluster,
                "owner": project.owner,
                "project_name": "test-project1",
            },
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["id"] == project.id
            assert payload["name"] == project.name
            assert payload["owner"] == project.owner
            assert payload["cluster"] == project.cluster
            assert payload["org_name"] == project.org_name
            assert payload["project_name"] == project.project_name

    async def test_shared_project_list(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: UserFactory,
        client: aiohttp.ClientSession,
        grant_project_permission: ProjectGranter,
    ) -> None:
        user1 = await regular_user_factory(project_name="test-project1")
        user2 = await regular_user_factory(project_name="test-project2")
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project1",
            },
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            data = await resp.json()
            project = Project(
                id=data["id"],
                cluster=data["cluster"],
                name=data["name"],
                org_name=data["org_name"],
                owner=data["owner"],
                project_name=data["project_name"],
            )
        await grant_project_permission(user2, project)
        async with client.get(
            url=neuro_flow_api.projects_url,
            params={
                "name": project.name,
                "cluster": project.cluster,
                "project_name": project.project_name,
            },
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            ids = {proj["id"] for proj in payload}
            assert project.id in ids

    async def test_projects_create_duplicate_fail(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPConflict.status_code, await resp.text()

    async def test_projects_create_duplicate_different_project_ok(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: UserFactory,
        client: aiohttp.ClientSession,
    ) -> None:
        user1 = await regular_user_factory(project_name="test-project1")
        user2 = await regular_user_factory(project_name="test-project2")
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project1",
            },
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project2",
            },
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()

    async def test_projects_create_duplicate_same_org_fail(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_org_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
                "org_name": "test-org",
            },
            headers=regular_org_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
                "org_name": "test-org",
            },
            headers=regular_org_user.headers,
        ) as resp:
            assert resp.status == HTTPConflict.status_code, await resp.text()

    async def test_projects_create_duplicate_different_org_ok(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: UserFactory,
        client: aiohttp.ClientSession,
    ) -> None:
        user1 = await regular_user_factory(project_name="test-project")
        user2 = await regular_user_factory(
            project_name="test-project", org_name="test-org"
        )
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
            },
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
                "org_name": "test-org",
            },
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()

    async def test_projects_list(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        for index in range(5):
            async with client.post(
                url=neuro_flow_api.projects_url,
                json={
                    "name": f"test-{index}",
                    "cluster": "test-cluster",
                    "project_name": "test-project",
                },
                headers=regular_user.headers,
            ) as resp:
                assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.get(
            url=neuro_flow_api.projects_url,
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            items = await resp.json()
            assert len(items) == 5
            names = set()
            for item in items:
                assert item["owner"] == regular_user.name
                assert item["cluster"] == "test-cluster"
                assert item["project_name"] == "test-project"
                names.add(item["name"])
            assert names == {f"test-{index}" for index in range(5)}

    async def test_projects_list_unclosed_connection(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        for index in range(200):
            async with client.post(
                url=neuro_flow_api.projects_url,
                json={
                    "name": f"test-{index}" + secrets.token_hex(200),
                    "cluster": "test-cluster",
                    "project_name": regular_user.name,
                },
                headers=regular_user.headers,
            ) as resp:
                assert resp.status == HTTPCreated.status_code, await resp.text()

        async def _list_partial() -> None:
            async with client.get(
                url=neuro_flow_api.projects_url,
                headers={
                    **regular_user.headers,
                    "Accept": "application/x-ndjson",
                },
            ) as resp:
                assert resp.status == HTTPOk.status_code, await resp.text()
                cnt = 0
                async for _ in resp.content:
                    if cnt:
                        await asyncio.sleep(0.01)
                        break
                    cnt += 1
            await asyncio.sleep(0.01)

        for _ in range(50):
            # Should not lock connection and block processing
            await asyncio.wait_for(_list_partial(), timeout=0.5)

    async def test_projects_list_only_owned(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: UserFactory,
        client: aiohttp.ClientSession,
    ) -> None:
        user1 = await regular_user_factory(project_name="test-project1")
        user2 = await regular_user_factory(project_name="test-project2")
        for user, project_name in [(user1, "test-project1"), (user2, "test-project2")]:
            for index in range(5):
                async with client.post(
                    url=neuro_flow_api.projects_url,
                    json={
                        "name": f"test-{index}",
                        "cluster": "test-cluster",
                        "project_name": project_name,
                    },
                    headers=user.headers,
                ) as resp:
                    assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.get(
            url=neuro_flow_api.projects_url,
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            items = await resp.json()
            assert len(items) == 5
            names = set()
            for item in items:
                assert item["owner"] == user1.name
                assert item["cluster"] == "test-cluster"
                names.add(item["name"])
            assert names == {f"test-{index}" for index in range(5)}

    async def test_projects_list_filter_by_org(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        regular_org_user: _User,
        grant_project_permission: ProjectGranter,
        org_name: str,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            data = await resp.json()
            project = Project(
                id=data["id"],
                cluster=data["cluster"],
                name=data["name"],
                org_name=data["org_name"],
                owner=data["owner"],
                project_name=data["project_name"],
            )

        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project",
                "org_name": org_name,
            },
            headers=regular_org_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            data = await resp.json()
            org_project = Project(
                id=data["id"],
                cluster=data["cluster"],
                name=data["name"],
                org_name=data["org_name"],
                owner=data["owner"],
                project_name=data["project_name"],
            )

        await grant_project_permission(regular_user, org_project)

        async with client.get(
            url=neuro_flow_api.projects_url,
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            items = await resp.json()
            assert len(items) == 2

        async with client.get(
            url=neuro_flow_api.projects_url,
            params={"org_name": ""},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            items = await resp.json()
            assert len(items) == 1
            assert items[0]["id"] == project.id

        async with client.get(
            url=neuro_flow_api.projects_url,
            params={"org_name": org_name},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            items = await resp.json()
            assert len(items) == 1
            assert items[0]["id"] == org_project.id

    async def test_projects_list_filter_by_project(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: UserFactory,
        grant_project_permission: ProjectGranter,
        client: aiohttp.ClientSession,
    ) -> None:
        user1 = await regular_user_factory(project_name="test-project1")
        user2 = await regular_user_factory(project_name="test-project2")
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project1",
            },
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            data = await resp.json()
            project1 = Project(
                id=data["id"],
                cluster=data["cluster"],
                name=data["name"],
                org_name=data["org_name"],
                owner=data["owner"],
                project_name=data["project_name"],
            )

        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": "test-project2",
            },
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            data = await resp.json()
            project2 = Project(
                id=data["id"],
                cluster=data["cluster"],
                name=data["name"],
                org_name=data["org_name"],
                owner=data["owner"],
                project_name=data["project_name"],
            )

        await grant_project_permission(user1, project2)

        async with client.get(
            url=neuro_flow_api.projects_url, headers=user1.headers
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            items = await resp.json()
            assert len(items) == 2

        async with client.get(
            url=neuro_flow_api.projects_url,
            params={"project_name": "test-project1"},
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            items = await resp.json()
            assert len(items) == 1
            assert items[0]["id"] == project1.id

    async def test_project_delete(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": "test",
                "cluster": "test-cluster",
                "project_name": regular_user.name,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            project_id = payload["id"]

        async with client.delete(
            url=neuro_flow_api.project_url(project_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPNoContent.status_code, await resp.text()

        async with client.get(
            url=neuro_flow_api.project_url(project_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()


class ProjectFactory(Protocol):
    async def __call__(
        self, user: _User, *, project_name: str | None = None
    ) -> Project:
        pass


@pytest.fixture
def project_factory(
    neuro_flow_api: NeuroFlowApiEndpoints,
    client: aiohttp.ClientSession,
) -> ProjectFactory:
    async def _factory(user: _User, *, project_name: str | None = None) -> Project:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={
                "name": secrets.token_hex(8),
                "cluster": "test-cluster",
                "project_name": project_name or "test-project",
            },
            headers=user.headers,
        ) as resp:
            payload = await resp.json()
            return Project(**payload)

    return _factory


class TestLiveJobsApi:
    async def test_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.live_jobs_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22"],
                "raw_id": "job-f50b735c-e087-41e6-bddc-4783bb4d14c1",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["yaml_id"] == "test-job"
            assert payload["project_id"] == project.id
            assert not payload["multi"]
            assert payload["tags"] == ["11", "22"]
            assert payload["raw_id"] == "job-f50b735c-e087-41e6-bddc-4783bb4d14c1"
            assert "id" in payload

    async def test_create_without_raw_id(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.live_jobs_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22"],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["yaml_id"] == "test-job"
            assert payload["project_id"] == project.id
            assert not payload["multi"]
            assert payload["tags"] == ["11", "22"]
            assert "id" in payload

    async def test_replace_new(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.put(
            url=neuro_flow_api.live_job_replace_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22"],
                "raw_id": "job-d0ec0e83-39bd-412e-8f1d-6d62d4c341a6",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["yaml_id"] == "test-job"
            assert payload["project_id"] == project.id
            assert not payload["multi"]
            assert payload["tags"] == ["11", "22"]
            assert payload["raw_id"] == "job-d0ec0e83-39bd-412e-8f1d-6d62d4c341a6"
            assert "id" in payload

    async def test_replace_existing(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.live_jobs_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22"],
                "raw_id": "job-f50b735c-e087-41e6-bddc-4783bb4d14c1",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["yaml_id"] == "test-job"
            assert payload["project_id"] == project.id
            assert not payload["multi"]
            assert payload["tags"] == ["11", "22"]
            assert payload["raw_id"] == "job-f50b735c-e087-41e6-bddc-4783bb4d14c1"
            assert "id" in payload
        async with client.put(
            url=neuro_flow_api.live_job_replace_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22", "33"],
                "raw_id": "job-d0ec0e83-39bd-412e-8f1d-6d62d4c341a6",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["yaml_id"] == "test-job"
            assert payload["project_id"] == project.id
            assert not payload["multi"]
            assert payload["tags"] == ["11", "22", "33"]
            assert payload["raw_id"] == "job-d0ec0e83-39bd-412e-8f1d-6d62d4c341a6"
            assert "id" in payload

    async def test_get_by_id(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.live_jobs_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22"],
                "raw_id": "job-f50b735c-e087-41e6-bddc-4783bb4d14c1",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            job_id = (await resp.json())["id"]
        async with client.get(
            url=neuro_flow_api.live_job_url(job_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["yaml_id"] == "test-job"
            assert payload["project_id"] == project.id
            assert not payload["multi"]
            assert payload["tags"] == ["11", "22"]
            assert payload["raw_id"] == "job-f50b735c-e087-41e6-bddc-4783bb4d14c1"
            assert payload["id"] == job_id

    async def test_get_by_yaml_id(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.live_jobs_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22"],
                "raw_id": "job-f50b735c-e087-41e6-bddc-4783bb4d14c1",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            job_id = (await resp.json())["id"]
        async with client.get(
            url=neuro_flow_api.live_job_by_yaml_id_url,
            params={"project_id": project.id, "yaml_id": "test-job"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["yaml_id"] == "test-job"
            assert payload["project_id"] == project.id
            assert not payload["multi"]
            assert payload["tags"] == ["11", "22"]
            assert payload["raw_id"] == "job-f50b735c-e087-41e6-bddc-4783bb4d14c1"
            assert payload["id"] == job_id

    async def test_create_duplicate_fail(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.live_jobs_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22"],
                "raw_id": "job-f50b735c-e087-41e6-bddc-4783bb4d14c1",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.post(
            url=neuro_flow_api.live_jobs_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22"],
                "raw_id": "job-f50b735c-e087-41e6-bddc-4783bb4d14c1",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPConflict.status_code, await resp.text()

    async def test_no_project_fail(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.live_jobs_url,
            json={
                "yaml_id": "test-job",
                "project_id": "not-exists",
                "multi": False,
                "tags": ["11", "22"],
                "raw_id": "job-f50b735c-e087-41e6-bddc-4783bb4d14c1",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()

    async def test_list(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        for index in range(5):
            async with client.post(
                url=neuro_flow_api.live_jobs_url,
                json={
                    "yaml_id": f"test-job-{index}",
                    "project_id": project.id,
                    "multi": False,
                    "tags": ["11", "22"],
                    "raw_id": "job-f50b735c-e087-41e6-bddc-" + str(index) * 12,
                },
                headers=regular_user.headers,
            ) as resp:
                assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.get(
            url=neuro_flow_api.live_jobs_url,
            params={"project_id": project.id},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            items = await resp.json()
            assert len(items) == 5
            yaml_ids = set()
            raw_ids = set()
            for item in items:
                assert item["project_id"] == project.id
                assert not item["multi"]
                assert item["tags"] == ["11", "22"]
                yaml_ids.add(item["yaml_id"])
                raw_ids.add(item["raw_id"])
            assert yaml_ids == {f"test-job-{index}" for index in range(5)}
            assert raw_ids == {
                f"job-f50b735c-e087-41e6-bddc-{str(index) * 12}" for index in range(5)
            }

    async def test_list_no_project(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.get(
            url=neuro_flow_api.live_jobs_url,
            params={"project_id": "random_value"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert await resp.json() == []

    async def test_projects_list_only_from_project(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project1 = await project_factory(regular_user)
        project2 = await project_factory(regular_user)
        for project in [project1, project2]:
            for index in range(5):
                async with client.post(
                    url=neuro_flow_api.live_jobs_url,
                    json={
                        "yaml_id": f"test-job-{index}",
                        "project_id": project.id,
                        "multi": False,
                        "tags": ["11", "22"],
                        "raw_id": "job-f50b735c-e087-41e6-bddc-" + str(index) * 12,
                    },
                    headers=regular_user.headers,
                ) as resp:
                    assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.get(
            url=neuro_flow_api.live_jobs_url,
            params={"project_id": project1.id},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            items = await resp.json()
            assert len(items) == 5
            yaml_ids = set()
            raw_ids = set()
            for item in items:
                assert item["project_id"] == project1.id
                assert not item["multi"]
                assert item["tags"] == ["11", "22"]
                yaml_ids.add(item["yaml_id"])
                raw_ids.add(item["raw_id"])
            assert yaml_ids == {f"test-job-{index}" for index in range(5)}
            assert raw_ids == {
                f"job-f50b735c-e087-41e6-bddc-{str(index) * 12}" for index in range(5)
            }


class TestBakeApi:
    async def test_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "tags": [],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["project_id"] == project.id
            assert payload["batch"] == "test-batch"
            assert payload["graphs"] == {"": {"a": [], "b": ["a"]}}
            assert payload["params"] == {"p1": "v1"}
            assert payload["tags"] == []
            assert "id" in payload
            assert "created_at" in payload

    async def test_create_shared_project(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: UserFactory,
        grant_project_permission: ProjectGranter,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        user1 = await regular_user_factory(project_name="test-project1")
        user2 = await regular_user_factory(project_name="test-project2")
        project = await project_factory(user1, project_name="test-project1")
        await grant_project_permission(user2, project, write=True)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "tags": [],
            },
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["project_id"] == project.id
            assert payload["batch"] == "test-batch"
            assert payload["graphs"] == {"": {"a": [], "b": ["a"]}}
            assert payload["params"] == {"p1": "v1"}
            assert payload["tags"] == []
            assert "id" in payload
            assert "created_at" in payload

    async def test_create_shared_read_only_project_forbidden(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: UserFactory,
        grant_project_permission: ProjectGranter,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        user1 = await regular_user_factory(project_name="test-project1")
        user2 = await regular_user_factory(project_name="test-project2")
        project = await project_factory(user1, project_name="test-project1")
        await grant_project_permission(user2, project, write=False)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "tags": ["foo", "bar"],
            },
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()

    async def test_create_with_meta(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "meta": {
                    "git_info": {
                        "sha": "test-sha",
                        "branch": "test-branch",
                        "tags": ["tag1", "tag2", "tag3"],
                    }
                },
                "tags": ["foo", "bar"],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["meta"] == {
                "git_info": {
                    "sha": "test-sha",
                    "branch": "test-branch",
                    "tags": ["tag1", "tag2", "tag3"],
                }
            }

    async def test_create_empty_meta(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "meta": {"git_info": None},
                "tags": ["foo", "bar"],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()

            assert payload["meta"] == {"git_info": None}

    async def test_get(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "tags": ["foo", "bar"],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload1 = await resp.json()

        bake_id = payload1["id"]

        async with client.get(
            url=neuro_flow_api.bake_url(bake_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["project_id"] == project.id
            assert payload["batch"] == "test-batch"
            assert payload["graphs"] == {"": {"a": [], "b": ["a"]}}
            assert payload["params"] == {"p1": "v1"}
            assert payload["tags"] == ["foo", "bar"]
            assert payload["id"] == bake_id
            assert payload["created_at"] == payload1["created_at"]
            assert payload["last_attempt"] is None

    async def test_get_by_name(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "tags": ["foo", "bar"],
                "name": "test-name",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload1 = await resp.json()

        bake_id = payload1["id"]

        async with client.get(
            url=neuro_flow_api.bake_by_name_url,
            params={"project_id": project.id, "name": "test-name"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["project_id"] == project.id
            assert payload["batch"] == "test-batch"
            assert payload["graphs"] == {"": {"a": [], "b": ["a"]}}
            assert payload["params"] == {"p1": "v1"}
            assert payload["tags"] == ["foo", "bar"]
            assert payload["id"] == bake_id
            assert payload["created_at"] == payload1["created_at"]
            assert payload["name"] == payload1["name"]
            assert payload["last_attempt"] is None

    async def test_list_empty(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.get(
            url=neuro_flow_api.bakes_url,
            params={
                "project_id": project.id,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == []

    async def test_list_no_project(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.get(
            url=neuro_flow_api.bakes_url,
            params={"project_id": "random_value"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert await resp.json() == []

    async def test_list_something(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "tags": [],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload1 = await resp.json()

        bake_id = payload1["id"]

        async with client.get(
            url=neuro_flow_api.bakes_url,
            params={
                "project_id": project.id,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == [
                {
                    "id": bake_id,
                    "project_id": project.id,
                    "batch": "test-batch",
                    "graphs": {"": {"a": [], "b": ["a"]}},
                    "params": {"p1": "v1"},
                    "created_at": payload1["created_at"],
                    "tags": [],
                    "meta": {"git_info": None},
                    "name": None,
                    "last_attempt": None,
                }
            ]

    async def test_list_by_tag(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "tags": ["tag1"],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload1 = await resp.json()

        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "tags": ["tag1", "tag2"],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload2 = await resp.json()

        bake1_id = payload1["id"]
        bake2_id = payload2["id"]

        async with client.get(
            url=neuro_flow_api.bakes_url,
            params={"project_id": project.id, "tags": ["tag1"]},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            ids = {item["id"] for item in payload}
            assert {bake1_id, bake2_id} == ids

        async with client.get(
            url=neuro_flow_api.bakes_url,
            params={"project_id": project.id, "tags": ["tag2"]},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            ids = {item["id"] for item in payload}
            assert {bake2_id} == ids

        async with client.get(
            url=neuro_flow_api.bakes_url,
            params={"project_id": project.id, "tags": ["tag1", "tag2"]},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            ids = {item["id"] for item in payload}
            assert {bake2_id} == ids

    async def test_list_since_until(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        bakes: list[tuple[str, datetime]] = []
        for _ in range(5):
            async with client.post(
                url=neuro_flow_api.bakes_url,
                json={
                    "project_id": project.id,
                    "batch": "test-batch",
                    "graphs": {"": {"a": [], "b": ["a"]}},
                    "params": {"p1": "v1"},
                    "tags": [],
                },
                headers=regular_user.headers,
            ) as resp:
                assert resp.status == HTTPCreated.status_code, await resp.text()
                payload = await resp.json()
                bakes.append(
                    (payload["id"], datetime.fromisoformat(payload["created_at"]))
                )

        bakes = list(reversed(bakes))

        for l in range(5):  # noqa: E741
            for r in range(l, 5):
                async with client.get(
                    url=neuro_flow_api.bakes_url,
                    params={
                        "project_id": project.id,
                        "since": bakes[r][1].isoformat(),
                        "until": bakes[l][1].isoformat(),
                        "reverse": "true",
                    },
                    headers=regular_user.headers,
                ) as resp:
                    assert resp.status == HTTPOk.status_code, await resp.text()
                    payload = await resp.json()
                    assert len(payload) == r - l + 1
                    for index, bake_data in enumerate(payload):
                        assert bake_data["id"] == bakes[l + index][0]


@pytest.fixture
def bake_factory(
    neuro_flow_api: NeuroFlowApiEndpoints,
    client: aiohttp.ClientSession,
    project_factory: ProjectFactory,
) -> Callable[[_User], Awaitable[Bake]]:
    async def _factory(user: _User) -> Bake:
        project = await project_factory(user)

        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
                "tags": ["tag"],
                "name": "test-name",
            },
            headers=user.headers,
        ) as resp:
            payload = await resp.json()
            return Bake(**payload)

    return _factory


class TestAttemptApi:
    CONFIGS_META = {
        "workspace": "workspace",
        "flow_config_id": "<flow_config_id>",
        "project_config_id": "<project_config_id>",
        "action_config_ids": {
            "action1": "<action1_config_id>",
            "action2": "<action2_config_id>",
        },
    }

    async def test_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["bake_id"] == bake.id
            assert payload["number"] == 1
            assert payload["result"] == "pending"
            assert payload["configs_meta"] == self.CONFIGS_META
            assert "id" in payload
            assert "created_at" in payload

    async def test_watcher_changes_status(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
        mock_platform_api_server: PlatformApiServer,
        config: Config,
    ) -> None:
        mock_platform_api_server.jobs.append(
            make_job(
                job_id="test-job-id",
                status=JobStatus.FAILED,
            )
        )

        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "executor_id": "test-job-id",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            attempt_id = payload["id"]

        await asyncio.sleep(config.watchers.polling_interval_sec * 2)
        print("42")
        # await asyncio.sleep(config.watchers.polling_interval_sec * 100)

        async with client.get(
            url=neuro_flow_api.attempt_url(attempt_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["result"] == "failed"

    async def test_list(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            attempt_id = payload["id"]
            created_at = payload["created_at"]

        async with client.get(
            url=neuro_flow_api.attempts_url,
            params={
                "bake_id": bake.id,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == [
                {
                    "id": attempt_id,
                    "bake_id": bake.id,
                    "number": 1,
                    "created_at": created_at,
                    "result": "pending",
                    "configs_meta": self.CONFIGS_META,
                    "executor_id": None,
                }
            ]

    async def test_list_no_bake(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.get(
            url=neuro_flow_api.attempts_url,
            params={
                "bake_id": "random value",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert await resp.json() == []

    async def test_get(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            attempt_id = payload["id"]
            created_at = payload["created_at"]

        async with client.get(
            url=neuro_flow_api.attempt_url(attempt_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": attempt_id,
                "bake_id": bake.id,
                "number": 1,
                "created_at": created_at,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
                "executor_id": None,
            }

    async def test_get_by_number(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
                "executor_id": "test-id",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            attempt_id = payload["id"]
            created_at = payload["created_at"]

        async with client.get(
            url=neuro_flow_api.attempt_by_number_url,
            params={
                "bake_id": bake.id,
                "number": 1,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": attempt_id,
                "bake_id": bake.id,
                "number": 1,
                "created_at": created_at,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
                "executor_id": "test-id",
            }

    async def test_get_last(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()

        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 2,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            attempt_id = payload["id"]
            created_at = payload["created_at"]

        async with client.get(
            url=neuro_flow_api.attempt_by_number_url,
            params={
                "bake_id": bake.id,
                "number": -1,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": attempt_id,
                "bake_id": bake.id,
                "number": 2,
                "created_at": created_at,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
                "executor_id": None,
            }

    async def test_replace(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            attempt_id = payload["id"]
            created_at = payload["created_at"]

        async with client.put(
            url=neuro_flow_api.attempt_replace_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "succeeded",
                "executor_id": "test_id",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()

        async with client.get(
            url=neuro_flow_api.attempt_url(attempt_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": attempt_id,
                "bake_id": bake.id,
                "number": 1,
                "created_at": created_at,
                "result": "succeeded",
                "configs_meta": self.CONFIGS_META,
                "executor_id": "test_id",
            }

    async def test_get_bake_with_last_attempt(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            attempt = await resp.json()

        async with client.get(
            url=neuro_flow_api.bake_url(bake.id),
            params={"fetch_last_attempt": "1"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["project_id"] == bake.project_id
            assert payload["batch"] == "test-batch"
            assert payload["id"] == bake.id
            assert payload["last_attempt"] == {
                "id": attempt["id"],
                "bake_id": bake.id,
                "number": 1,
                "created_at": attempt["created_at"],
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
                "executor_id": None,
            }

    async def test_get_bake_by_name_with_last_attempt(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            attempt = await resp.json()

        async with client.get(
            url=neuro_flow_api.bake_by_name_url,
            params={
                "project_id": bake.project_id,
                "name": "test-name",
                "fetch_last_attempt": "1",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["project_id"] == bake.project_id
            assert payload["batch"] == "test-batch"
            assert payload["id"] == bake.id
            assert payload["last_attempt"] == {
                "id": attempt["id"],
                "bake_id": bake.id,
                "number": 1,
                "created_at": attempt["created_at"],
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
                "executor_id": None,
            }

    async def test_list_bakes_with_last_attempt(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "configs_meta": self.CONFIGS_META,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            attempt = await resp.json()

        async with client.get(
            url=neuro_flow_api.bakes_url,
            params={
                "project_id": bake.project_id,
                "fetch_last_attempt": "1",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == [
                {
                    "id": bake.id,
                    "project_id": bake.project_id,
                    "batch": "test-batch",
                    "graphs": {"": {"a": [], "b": ["a"]}},
                    "params": {"p1": "v1"},
                    "created_at": bake.created_at,
                    "tags": ["tag"],
                    "name": "test-name",
                    "meta": {"git_info": None},
                    "last_attempt": {
                        "id": attempt["id"],
                        "bake_id": bake.id,
                        "number": 1,
                        "created_at": attempt["created_at"],
                        "result": "pending",
                        "configs_meta": self.CONFIGS_META,
                        "executor_id": None,
                    },
                }
            ]


class TestConfigFileApi:
    async def test_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.config_files_url,
            json={
                "bake_id": bake.id,
                "filename": "batch.yaml",
                "content": "<batch content>",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["bake_id"] == bake.id
            assert payload["filename"] == "batch.yaml"
            assert payload["content"] == "<batch content>"
            assert "id" in payload

    async def test_get(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.config_files_url,
            json={
                "bake_id": bake.id,
                "filename": "batch.yaml",
                "content": "<batch content>",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            config_file_id = payload["id"]

        async with client.get(
            url=neuro_flow_api.config_file_url(config_file_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["id"] == config_file_id
            assert payload["bake_id"] == bake.id
            assert payload["filename"] == "batch.yaml"
            assert payload["content"] == "<batch content>"


@pytest.fixture
def attempt_factory(
    neuro_flow_api: NeuroFlowApiEndpoints,
    client: aiohttp.ClientSession,
    bake_factory: Callable[[_User], Awaitable[Bake]],
) -> Callable[[_User], Awaitable[Attempt]]:
    configs_meta = {
        "workspace": "workspace",
        "flow_config_id": "<flow_config_id>",
        "project_config_id": "<project_config_id>",
        "action_config_ids": {
            "action1": "<action1_config_id>",
            "action2": "<action2_config_id>",
        },
    }

    async def _factory(user: _User) -> Attempt:
        bake = await bake_factory(user)

        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
                "result": "pending",
                "configs_meta": configs_meta,
            },
            headers=user.headers,
        ) as resp:
            payload = await resp.json()
            return Attempt(**payload)

    return _factory


class TestTaskApi:
    async def test_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        attempt_factory: Callable[[_User], Awaitable[Attempt]],
    ) -> None:
        attempt = await attempt_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.tasks_url,
            json={
                "yaml_id": "a",
                "attempt_id": attempt.id,
                "raw_id": "",
                "outputs": {},
                "state": {},
                "statuses": [],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["yaml_id"] == "a"
            assert payload["attempt_id"] == attempt.id
            assert payload["raw_id"] == ""
            assert payload["outputs"] == {}
            assert payload["state"] == {}
            assert payload["statuses"] == []
            assert "id" in payload

    async def test_list(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        attempt_factory: Callable[[_User], Awaitable[Attempt]],
    ) -> None:
        attempt = await attempt_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.tasks_url,
            json={
                "yaml_id": "a",
                "attempt_id": attempt.id,
                "raw_id": "",
                "outputs": {},
                "state": {},
                "statuses": [],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            task_id = payload["id"]

        async with client.get(
            url=neuro_flow_api.tasks_url,
            params={
                "attempt_id": attempt.id,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == [
                {
                    "id": task_id,
                    "attempt_id": attempt.id,
                    "yaml_id": "a",
                    "raw_id": "",
                    "outputs": {},
                    "state": {},
                    "statuses": [],
                }
            ]

    async def test_list_no_attempt(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.get(
            url=neuro_flow_api.tasks_url,
            params={
                "attempt_id": "random value",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert await resp.json() == []

    async def test_get(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        attempt_factory: Callable[[_User], Awaitable[Attempt]],
    ) -> None:
        attempt = await attempt_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.tasks_url,
            json={
                "yaml_id": "a",
                "attempt_id": attempt.id,
                "raw_id": "",
                "outputs": {},
                "state": {},
                "statuses": [],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            task_id = payload["id"]

        async with client.get(
            url=neuro_flow_api.task_url(task_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": task_id,
                "attempt_id": attempt.id,
                "yaml_id": "a",
                "raw_id": "",
                "outputs": {},
                "state": {},
                "statuses": [],
            }

    async def test_get_by_yaml_id(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        attempt_factory: Callable[[_User], Awaitable[Attempt]],
    ) -> None:
        attempt = await attempt_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.tasks_url,
            json={
                "yaml_id": "a",
                "attempt_id": attempt.id,
                "raw_id": "",
                "outputs": {},
                "state": {},
                "statuses": [],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            task_id = payload["id"]

        async with client.get(
            url=neuro_flow_api.task_by_yaml_id_url,
            params={
                "attempt_id": attempt.id,
                "yaml_id": "a",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": task_id,
                "attempt_id": attempt.id,
                "yaml_id": "a",
                "raw_id": "",
                "outputs": {},
                "state": {},
                "statuses": [],
            }

    async def test_replace(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        attempt_factory: Callable[[_User], Awaitable[Attempt]],
    ) -> None:
        attempt = await attempt_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.tasks_url,
            json={
                "yaml_id": "a",
                "attempt_id": attempt.id,
                "raw_id": "",
                "outputs": {},
                "state": {},
                "statuses": [],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            task_id = payload["id"]

        async with client.put(
            url=neuro_flow_api.task_replace_url,
            json={
                "attempt_id": attempt.id,
                "yaml_id": "a",
                "raw_id": "job-41628b21-d321-454f-abe0-630e5bc38abf",
                "outputs": {},
                "state": {},
                "statuses": [{"created_at": attempt.created_at, "status": "pending"}],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()

        async with client.get(
            url=neuro_flow_api.task_by_yaml_id_url,
            params={
                "attempt_id": attempt.id,
                "yaml_id": "a",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": task_id,
                "attempt_id": attempt.id,
                "yaml_id": "a",
                "raw_id": "job-41628b21-d321-454f-abe0-630e5bc38abf",
                "outputs": {},
                "state": {},
                "statuses": [{"created_at": attempt.created_at, "status": "pending"}],
            }


class TestCacheEntryApi:
    def make_payload(self, project_id: str) -> dict[str, Any]:
        return {
            "project_id": project_id,
            "task_id": "test.task",
            "batch": "seq",
            "key": "key",
            "raw_id": "test",
            "outputs": {"foo": "bar"},
            "state": {"foo_state": "bar_state"},
        }

    async def test_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        request_payload = self.make_payload(project.id)
        async with client.post(
            url=neuro_flow_api.cache_entries_url,
            json=request_payload,
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert "id" in payload
            for key in request_payload:
                assert payload[key] == request_payload[key]
            created_at = datetime.fromisoformat(payload["created_at"])
            assert created_at

    async def test_get_by_id(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        request_payload = self.make_payload(project.id)
        async with client.post(
            url=neuro_flow_api.cache_entries_url,
            json=request_payload,
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            entry_id = (await resp.json())["id"]
        async with client.get(
            url=neuro_flow_api.cache_entry_url(entry_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["id"] == entry_id
            for key in request_payload:
                assert payload[key] == request_payload[key]
            created_at = datetime.fromisoformat(payload["created_at"])
            assert created_at

    async def test_get_by_key(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        request_payload = self.make_payload(project.id)
        async with client.post(
            url=neuro_flow_api.cache_entries_url,
            json=request_payload,
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            entry_id = (await resp.json())["id"]
        async with client.get(
            url=neuro_flow_api.cache_entry_by_key_url,
            params={
                "project_id": project.id,
                "task_id": request_payload["task_id"],
                "batch": request_payload["batch"],
                "key": request_payload["key"],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["id"] == entry_id
            for key in request_payload:
                assert payload[key] == request_payload[key]
            created_at = datetime.fromisoformat(payload["created_at"])
            assert created_at

    async def test_create_duplicate_ok(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        request_payload = self.make_payload(project.id)
        async with client.post(
            url=neuro_flow_api.cache_entries_url,
            json=request_payload,
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
        payload2 = dict(request_payload)
        payload2["outputs"] = {"bazz": "egg"}
        async with client.post(
            url=neuro_flow_api.cache_entries_url,
            json=payload2,
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload3 = await resp.json()
            assert payload3["state"] == request_payload["state"]
            assert payload3["outputs"] == payload2["outputs"]

    async def test_no_project_fail(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        request_payload = self.make_payload("not-exists")
        async with client.post(
            url=neuro_flow_api.cache_entries_url,
            json=request_payload,
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()

    async def test_delete(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project1 = await project_factory(regular_user)
        project2 = await project_factory(regular_user)
        project_to_entry_id: dict[str, str] = {}
        for project in [project1, project2]:
            request_payload = self.make_payload(project.id)
            async with client.post(
                url=neuro_flow_api.cache_entries_url,
                json=request_payload,
                headers=regular_user.headers,
            ) as resp:
                assert resp.status == HTTPCreated.status_code, await resp.text()
                project_to_entry_id[project.id] = (await resp.json())["id"]
        async with client.delete(
            url=neuro_flow_api.cache_entries_url,
            params={"project_id": project1.id},
            headers=regular_user.headers,
        ):
            pass
        async with client.get(
            url=neuro_flow_api.cache_entry_url(project_to_entry_id[project1.id]),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()

        async with client.get(
            url=neuro_flow_api.cache_entry_url(project_to_entry_id[project2.id]),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()

    async def test_delete_single_task(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: ProjectFactory,
    ) -> None:
        project = await project_factory(regular_user)
        entries = []
        for index in range(2):
            request_payload = self.make_payload(project.id)
            request_payload["task_id"] = f"test.task{index}"
            async with client.post(
                url=neuro_flow_api.cache_entries_url,
                json=request_payload,
                headers=regular_user.headers,
            ) as resp:
                assert resp.status == HTTPCreated.status_code, await resp.text()
                entries.append(await resp.json())
        async with client.delete(
            url=neuro_flow_api.cache_entries_url,
            params={"project_id": project.id, "task_id": entries[0]["task_id"]},
            headers=regular_user.headers,
        ):
            pass
        async with client.get(
            url=neuro_flow_api.cache_entry_url(entries[0]["id"]),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()

        async with client.get(
            url=neuro_flow_api.cache_entry_url(entries[1]["id"]),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()


class TestBakeImagesApi:
    async def test_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bake_images_url,
            json={
                "bake_id": bake.id,
                "ref": "image:test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "pending",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["bake_id"] == bake.id
            assert payload["ref"] == "image:test"
            assert payload["yaml_defs"] == ["foo.bar.test"]
            assert payload["context_on_storage"] == "storage://default/user/ctx"
            assert payload["dockerfile_rel"] == "Dockerfile"
            assert payload["status"] == "pending"
            assert payload["builder_job_id"] is None
            assert "id" in payload

    async def test_create_cached(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bake_images_url,
            json={
                "bake_id": bake.id,
                "ref": "image:test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "cached",
                "builder_job_id": "job-id",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["bake_id"] == bake.id
            assert payload["ref"] == "image:test"
            assert payload["yaml_defs"] == ["foo.bar.test"]
            assert payload["context_on_storage"] == "storage://default/user/ctx"
            assert payload["dockerfile_rel"] == "Dockerfile"
            assert payload["status"] == "cached"
            assert payload["builder_job_id"] == "job-id"
            assert "id" in payload

    async def test_create_old_api(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bake_images_url,
            json={
                "bake_id": bake.id,
                "ref": "image:test",
                "prefix": "foo.bar",
                "yaml_id": "test",
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "pending",
                "yaml_defs": ["foo.bar.test"],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["bake_id"] == bake.id
            assert payload["ref"] == "image:test"
            assert payload["prefix"] == "foo.bar"
            assert payload["yaml_id"] == "test"
            assert payload["context_on_storage"] == "storage://default/user/ctx"
            assert payload["dockerfile_rel"] == "Dockerfile"
            assert payload["status"] == "pending"
            assert payload["builder_job_id"] is None
            assert "id" in payload

    async def test_list(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bake_images_url,
            json={
                "bake_id": bake.id,
                "ref": "image:test",
                "yaml_defs": ["foo.bar.test"],
                "status": "pending",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            image_id = payload["id"]

        async with client.get(
            url=neuro_flow_api.bake_images_url,
            params={
                "bake_id": bake.id,
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == [
                {
                    "id": image_id,
                    "bake_id": bake.id,
                    "ref": "image:test",
                    "prefix": "foo.bar",
                    "yaml_id": "test",
                    "yaml_defs": ["foo.bar.test"],
                    "context_on_storage": None,
                    "dockerfile_rel": None,
                    "status": "pending",
                    "builder_job_id": None,
                }
            ]

    async def test_list_no_bake(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.get(
            url=neuro_flow_api.bake_images_url,
            params={
                "bake_id": "random value",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert await resp.json() == []

    async def test_get(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bake_images_url,
            json={
                "bake_id": bake.id,
                "ref": "image:test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "pending",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            image_id = payload["id"]

        async with client.get(
            url=neuro_flow_api.bake_image_url(image_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": image_id,
                "bake_id": bake.id,
                "ref": "image:test",
                "prefix": "foo.bar",
                "yaml_id": "test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "pending",
                "builder_job_id": None,
            }

    async def test_get_by_ref(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bake_images_url,
            json={
                "bake_id": bake.id,
                "ref": "image:test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "pending",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            image_id = payload["id"]

        async with client.get(
            url=neuro_flow_api.bake_image_by_ref,
            params={
                "bake_id": bake.id,
                "ref": "image:test",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": image_id,
                "bake_id": bake.id,
                "ref": "image:test",
                "prefix": "foo.bar",
                "yaml_id": "test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "pending",
                "builder_job_id": None,
            }

    async def test_replace(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Bake]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bake_images_url,
            json={
                "bake_id": bake.id,
                "ref": "image:test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "pending",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            image_id = payload["id"]

        async with client.patch(
            url=neuro_flow_api.bake_image_url(image_id),
            json={
                "status": "building",
                "builder_job_id": "job-id-here",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": image_id,
                "bake_id": bake.id,
                "ref": "image:test",
                "prefix": "foo.bar",
                "yaml_id": "test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "building",
                "builder_job_id": "job-id-here",
            }

        async with client.get(
            url=neuro_flow_api.bake_image_url(image_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": image_id,
                "bake_id": bake.id,
                "ref": "image:test",
                "prefix": "foo.bar",
                "yaml_id": "test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "building",
                "builder_job_id": "job-id-here",
            }

        async with client.patch(
            url=neuro_flow_api.bake_image_url(image_id),
            json={
                "status": "built",
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": image_id,
                "bake_id": bake.id,
                "ref": "image:test",
                "prefix": "foo.bar",
                "yaml_id": "test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "built",
                "builder_job_id": "job-id-here",
            }

        async with client.get(
            url=neuro_flow_api.bake_image_url(image_id),
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload == {
                "id": image_id,
                "bake_id": bake.id,
                "ref": "image:test",
                "prefix": "foo.bar",
                "yaml_id": "test",
                "yaml_defs": ["foo.bar.test"],
                "context_on_storage": "storage://default/user/ctx",
                "dockerfile_rel": "Dockerfile",
                "status": "built",
                "builder_job_id": "job-id-here",
            }
