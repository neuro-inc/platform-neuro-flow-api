import secrets
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, AsyncIterator, Awaitable, Callable, Dict

import aiohttp
import pytest
from aiohttp.web import HTTPOk
from aiohttp.web_exceptions import (
    HTTPConflict,
    HTTPCreated,
    HTTPForbidden,
    HTTPNotFound,
    HTTPUnauthorized,
)

from platform_neuro_flow_api.api import create_app
from platform_neuro_flow_api.config import Config
from platform_neuro_flow_api.storage.base import Bake, Project

from .auth import _User
from .conftest import ApiAddress, create_local_app_server


pytestmark = pytest.mark.asyncio


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
        return f"{self.server_base_url}/api/docs/v1/flow/swagger.json"

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
    def attempts_url(self) -> str:
        return f"{self.server_base_url}/api/v1/flow/attempts"

    def attempt_url(self, id: str) -> str:
        return f"{self.attempts_url}/{id}"

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

    async def test_ping_allowed_origin(
        self, neuro_flow_api: NeuroFlowApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(
            neuro_flow_api.ping_url, headers={"Origin": "https://neu.ro"}
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert resp.headers["Access-Control-Allow-Origin"] == "https://neu.ro"
            assert resp.headers["Access-Control-Allow-Credentials"] == "true"

    async def test_ping_options_no_headers(
        self, neuro_flow_api: NeuroFlowApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(neuro_flow_api.ping_url) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert await resp.text() == (
                "CORS preflight request failed: "
                "origin header is not specified in the request"
            )

    async def test_ping_options_unknown_origin(
        self, neuro_flow_api: NeuroFlowApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(
            neuro_flow_api.ping_url,
            headers={
                "Origin": "http://unknown",
                "Access-Control-Request-Method": "GET",
            },
        ) as resp:
            assert resp.status == HTTPForbidden.status_code, await resp.text()
            assert await resp.text() == (
                "CORS preflight request failed: "
                "origin 'http://unknown' is not allowed"
            )

    async def test_ping_options(
        self, neuro_flow_api: NeuroFlowApiEndpoints, client: aiohttp.ClientSession
    ) -> None:
        async with client.options(
            neuro_flow_api.ping_url,
            headers={
                "Origin": "https://neu.ro",
                "Access-Control-Request-Method": "GET",
            },
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            assert resp.headers["Access-Control-Allow-Origin"] == "https://neu.ro"
            assert resp.headers["Access-Control-Allow-Credentials"] == "true"
            assert resp.headers["Access-Control-Allow-Methods"] == "GET"


class TestProjectsApi:
    async def test_projects_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={"name": "test", "cluster": "test-cluster"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["name"] == "test"
            assert payload["owner"] == regular_user.name
            assert payload["cluster"] == "test-cluster"
            assert "id" in payload

    async def test_projects_get_by_id(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={"name": "test", "cluster": "test-cluster"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            project_id = (await resp.json())["id"]
        async with client.get(
            url=neuro_flow_api.project_url(project_id),
            json={"name": "test"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["name"] == "test"
            assert payload["owner"] == regular_user.name
            assert payload["cluster"] == "test-cluster"
            assert "id" in payload

    async def test_projects_get_by_name(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={"name": "test", "cluster": "test-cluster"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            project_id = (await resp.json())["id"]
        async with client.get(
            url=neuro_flow_api.project_by_name_url,
            params={"name": "test", "cluster": "test-cluster"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPOk.status_code, await resp.text()
            payload = await resp.json()
            assert payload["name"] == "test"
            assert payload["owner"] == regular_user.name
            assert payload["cluster"] == "test-cluster"
            assert payload["id"] == project_id

    async def test_projects_create_duplicate_fail(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={"name": "test", "cluster": "test-cluster"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={"name": "test", "cluster": "test-cluster"},
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPConflict.status_code, await resp.text()

    async def test_projects_list(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
    ) -> None:
        for index in range(5):
            async with client.post(
                url=neuro_flow_api.projects_url,
                json={"name": f"test-{index}", "cluster": "test-cluster"},
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
                names.add(item["name"])
            assert names == {f"test-{index}" for index in range(5)}

    async def test_projects_list_only_owned(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: Callable[[], Awaitable[_User]],
        client: aiohttp.ClientSession,
    ) -> None:
        user1 = await regular_user_factory()
        user2 = await regular_user_factory()
        for user in [user1, user2]:
            for index in range(5):
                async with client.post(
                    url=neuro_flow_api.projects_url,
                    json={"name": f"test-{index}", "cluster": "test-cluster"},
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


@pytest.fixture()
def project_factory(
    neuro_flow_api: NeuroFlowApiEndpoints,
    client: aiohttp.ClientSession,
) -> Callable[[_User], Awaitable[Project]]:
    async def _factory(user: _User) -> Project:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={"name": secrets.token_hex(8), "cluster": "test-cluster"},
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
        project_factory: Callable[[_User], Awaitable[Project]],
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
        project_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        project = await project_factory(regular_user)
        async with client.put(
            url=neuro_flow_api.live_job_replace_url,
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

    async def test_replace_existing(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
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
        async with client.put(
            url=neuro_flow_api.live_job_replace_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22", "33"],
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["yaml_id"] == "test-job"
            assert payload["project_id"] == project.id
            assert not payload["multi"]
            assert payload["tags"] == ["11", "22", "33"]
            assert "id" in payload

    async def test_get_by_id(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
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
            assert payload["id"] == job_id

    async def test_get_by_yaml_id(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
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
            assert payload["id"] == job_id

    async def test_create_duplicate_fail(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
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
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()

    async def test_list(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
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
            for item in items:
                assert item["project_id"] == project.id
                assert not item["multi"]
                assert item["tags"] == ["11", "22"]
                yaml_ids.add(item["yaml_id"])
            assert yaml_ids == {f"test-job-{index}" for index in range(5)}

    async def test_projects_list_only_from_project(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
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
            for item in items:
                assert item["project_id"] == project1.id
                assert not item["multi"]
                assert item["tags"] == ["11", "22"]
                yaml_ids.add(item["yaml_id"])
            assert yaml_ids == {f"test-job-{index}" for index in range(5)}

    async def test_projects_cannot_access_not_owner(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: Callable[[], Awaitable[_User]],
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        user1 = await regular_user_factory()
        user2 = await regular_user_factory()
        project = await project_factory(user1)
        async with client.post(
            url=neuro_flow_api.live_jobs_url,
            json={
                "yaml_id": "test-job",
                "project_id": project.id,
                "multi": False,
                "tags": ["11", "22"],
            },
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            job_id = (await resp.json())["id"]
        # Cannot get by id
        async with client.get(
            url=neuro_flow_api.live_job_url(job_id),
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()
        # Cannot get by yaml id
        async with client.get(
            url=neuro_flow_api.live_job_by_yaml_id_url,
            params={"project_id": project.id, "yaml_id": "test-job"},
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()
        # Cannot list
        async with client.get(
            url=neuro_flow_api.live_jobs_url,
            params={"project_id": project.id},
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()


class TestBakeApi:
    async def test_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
            },
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            payload = await resp.json()
            assert payload["project_id"] == project.id
            assert payload["batch"] == "test-batch"
            assert payload["graphs"] == {"": {"a": [], "b": ["a"]}}
            assert payload["params"] == {"p1": "v1"}
            assert "id" in payload
            assert "created_at" in payload

    async def test_get(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
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
            assert payload["id"] == bake_id
            assert payload["created_at"] == payload1["created_at"]

    async def test_list_empty(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
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

    async def test_list_something(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        project = await project_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
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
                }
            ]


@pytest.fixture()
def bake_factory(
    neuro_flow_api: NeuroFlowApiEndpoints,
    client: aiohttp.ClientSession,
    project_factory: Callable[[_User], Awaitable[Project]],
) -> Callable[[_User], Awaitable[Bake]]:
    async def _factory(user: _User) -> Bake:
        async with client.post(
            url=neuro_flow_api.projects_url,
            json={"name": secrets.token_hex(8), "cluster": "test-cluster"},
            headers=user.headers,
        ) as resp:
            payload = await resp.json()
            project = Project(**payload)

        async with client.post(
            url=neuro_flow_api.bakes_url,
            json={
                "project_id": project.id,
                "batch": "test-batch",
                "graphs": {"": {"a": [], "b": ["a"]}},
                "params": {"p1": "v1"},
            },
            headers=user.headers,
        ) as resp:
            payload2 = await resp.json()
            return Bake(**payload2)

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
        bake_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
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

    async def test_list(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
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
                }
            ]

    async def test_get(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
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
            }

    async def test_get_by_number(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        bake_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        bake = await bake_factory(regular_user)
        async with client.post(
            url=neuro_flow_api.attempts_url,
            json={
                "bake_id": bake.id,
                "number": 1,
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
            params={
                "bake_id": bake.id,
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
            }


class TestCacheEntryApi:
    def make_payload(self, project_id: str) -> Dict[str, Any]:
        return {
            "project_id": project_id,
            "task_id": "test.task",
            "batch": "seq",
            "key": "key",
            "outputs": {"foo": "bar"},
            "state": {"foo_state": "bar_state"},
        }

    async def test_create(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
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
        project_factory: Callable[[_User], Awaitable[Project]],
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
        project_factory: Callable[[_User], Awaitable[Project]],
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

    async def test_create_duplicate_fail(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user: _User,
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        project = await project_factory(regular_user)
        request_payload = self.make_payload(project.id)
        async with client.post(
            url=neuro_flow_api.cache_entries_url,
            json=request_payload,
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
        async with client.post(
            url=neuro_flow_api.cache_entries_url,
            json=request_payload,
            headers=regular_user.headers,
        ) as resp:
            assert resp.status == HTTPConflict.status_code, await resp.text()

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
        project_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        project1 = await project_factory(regular_user)
        project2 = await project_factory(regular_user)
        project_to_entry_id: Dict[str, str] = {}
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
        ) as resp:
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

    async def test_cannot_access_not_owner(
        self,
        neuro_flow_api: NeuroFlowApiEndpoints,
        regular_user_factory: Callable[[], Awaitable[_User]],
        client: aiohttp.ClientSession,
        project_factory: Callable[[_User], Awaitable[Project]],
    ) -> None:
        user1 = await regular_user_factory()
        user2 = await regular_user_factory()
        project = await project_factory(user1)
        request_payload = self.make_payload(project.id)
        async with client.post(
            url=neuro_flow_api.cache_entries_url,
            json=request_payload,
            headers=user1.headers,
        ) as resp:
            assert resp.status == HTTPCreated.status_code, await resp.text()
            entry_id = (await resp.json())["id"]
        # Cannot get by id
        async with client.get(
            url=neuro_flow_api.cache_entry_url(entry_id),
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()
        # Cannot get by yaml key
        async with client.get(
            url=neuro_flow_api.cache_entry_by_key_url,
            params={
                "project_id": project.id,
                "task_id": request_payload["task_id"],
                "batch": request_payload["batch"],
                "key": request_payload["key"],
            },
            headers=user2.headers,
        ) as resp:
            assert resp.status == HTTPNotFound.status_code, await resp.text()
