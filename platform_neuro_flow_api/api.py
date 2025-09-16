from __future__ import annotations

import abc
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from datetime import datetime
from typing import Any

import aiohttp
import aiohttp.web
from aiohttp import web
from aiohttp.web import (
    AppKey,
    Application,
    HTTPBadRequest,
    HTTPInternalServerError,
    HTTPUnauthorized,
    Request,
    Response,
    StreamResponse,
    delete,
    get,
    json_response,
    middleware,
    patch,
    post,
    put,
)
from aiohttp.web_exceptions import (
    HTTPConflict,
    HTTPCreated,
    HTTPNoContent,
    HTTPNotFound,
    HTTPOk,
    HTTPUnprocessableEntity,
)
from aiohttp_security import check_authorized
from apolo_api_client import ApiClient as PlatformApiClient
from marshmallow import ValidationError
from neuro_auth_client import AuthClient, Permission, check_permissions
from neuro_auth_client.security import AuthScheme, setup_security
from neuro_logging import (
    init_logging,
    setup_sentry,
)

from platform_neuro_flow_api import APP_NAME, __version__
from platform_neuro_flow_api.identity import untrusted_user

from .config import Config, PlatformAuthConfig
from .config_factory import EnvironConfigFactory
from .postgres import make_async_engine
from .project_deleter import ProjectDeleter
from .schema import (
    AttemptSchema,
    BakeImagePatchSchema,
    BakeImageSchema,
    BakeSchema,
    CacheEntrySchema,
    ConfigFileSchema,
    LiveJobSchema,
    ProjectSchema,
    TaskSchema,
)
from .storage.base import (
    Attempt,
    Bake,
    ExistsError,
    NotExistsError,
    Project,
    Storage,
    _Sentinel,
    sentinel,
)
from .storage.postgres import PostgresStorage
from .utils import auto_close, ndjson_error_handler
from .watchers import ExecutorAliveWatcher, WatchersPoller

logger = logging.getLogger(__name__)

CONFIG: AppKey[Config] = AppKey("CONFIG", Config)
API_V1_APP: AppKey[Application] = AppKey("API_V1_APP", Application)
LIVE_JOBS_APP: AppKey[Application] = AppKey("LIVE_JOBS_APP", Application)
PROJECTS_APP: AppKey[Application] = AppKey("PROJECTS_APP", Application)
BAKES_APP: AppKey[Application] = AppKey("BAKES_APP", Application)
ATTEMPTS_APP: AppKey[Application] = AppKey("ATTEMPTS_APP", Application)
TASKS_APP: AppKey[Application] = AppKey("TASKS_APP", Application)
CACHE_ENTRIES_APP: AppKey[Application] = AppKey("CACHE_ENTRIES_APP", Application)
CONFIG_FILES_APP: AppKey[Application] = AppKey("CONFIG_FILES_APP", Application)
BAKE_IMAGES_APP: AppKey[Application] = AppKey("BAKE_IMAGES_APP", Application)
STORAGE: AppKey[Storage] = AppKey("STORAGE", Storage)
AUTH_CLIENT: AppKey[AuthClient] = AppKey("AUTH_CLIENT", AuthClient)
HANDLER: AppKey[Any] = AppKey("handler", None)


def parse_iso8601(s: str) -> datetime:
    return datetime.fromisoformat(s)


def accepts_ndjson(request: aiohttp.web.Request) -> bool:
    accept = request.headers.get("Accept", "")
    return "application/x-ndjson" in accept


class ProjectAccessMixin:
    @property
    @abc.abstractmethod
    def storage(self) -> Storage:
        pass

    def _get_projects_uri(
        self, project_name: str, cluster_name: str, org_name: str | None
    ) -> str:
        uri = f"flow://{cluster_name}"
        if org_name:
            uri += f"/{org_name}"
        return uri + f"/{project_name}"

    def _get_projects_write_perm(
        self, project_name: str, cluster_name: str, org_name: str | None
    ) -> Permission:
        return Permission(
            self._get_projects_uri(project_name, cluster_name, org_name), "write"
        )

    def _get_project_uris(self, project: Project) -> list[str]:
        base = f"flow://{project.cluster}"
        if project.org_name:
            base += f"/{project.org_name}"
        base += f"/{project.project_name}"
        return [base + f"/{project.id}", base + f"/{project.name}"]

    def _get_project_read_perms(self, project: Project) -> list[Permission]:
        return [Permission(uri, "read") for uri in self._get_project_uris(project)]

    def _get_project_write_perms(self, project: Project) -> list[Permission]:
        return [Permission(uri, "write") for uri in self._get_project_uris(project)]

    async def _check_project_access(
        self, request: aiohttp.web.Request, project: Project, *, write: bool
    ) -> None:
        if write:
            await check_permissions(request, [self._get_project_write_perms(project)])
        else:
            await check_permissions(request, [self._get_project_read_perms(project)])

    async def _get_project(
        self, request: aiohttp.web.Request, project_id: str, *, write: bool
    ) -> Project:
        try:
            project = await self.storage.projects.get(project_id)
        except NotExistsError:
            raise HTTPNotFound
        await self._check_project_access(request, project, write=write)
        return project


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> None:
        app.router.add_get("/ping", self.handle_ping)
        app.router.add_get("/secured-ping", self.handle_secured_ping)

    async def handle_ping(self, request: Request) -> Response:
        """
        ---
        summary: Ping endpoint
        tags:
          - Misc
        responses:
          '200':
            description: Returns Pong
        """
        return Response(text="Pong")

    async def handle_secured_ping(self, request: Request) -> Response:
        """
        ---
        summary: Secured ping
        description: Requires authentication
        security:
          - jwt: []
        tags:
          - Misc
        responses:
          '200':
            description: Returns Secured Pong
          '401':
            description: Unauthorized
        """
        await check_authorized(request)
        return Response(text="Secured Pong")


class ProjectsApiHandler(ProjectAccessMixin):
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.router.add_get("", self.list_projects)
        app.router.add_post("", self.create_project)
        app.router.add_get("/by_name", self.get_project_by_name)
        app.router.add_get("/{id}", self.get_project)
        app.router.add_delete("/{id}", self.delete_project)

    @property
    def storage(self) -> Storage:
        return self._app[STORAGE]

    @property
    def auth_client(self) -> AuthClient:
        return self._app[AUTH_CLIENT]

    async def list_projects(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        """
        ---
        summary: List all users projects
        description: Replaces older @docs and uses NDJSON or JSON
        tags:
          - projects
        parameters:
          - name: name
            in: query
            schema:
              type: string
          - name: cluster
            in: query
            schema:
              type: string
          - name: org_name
            in: query
            schema:
              type: string
              nullable: true
          - name: owner
            in: query
            schema:
              type: string
          - name: project_name
            in: query
            schema:
              type: string
        responses:
          '200':
            description: List of projects
            content:
              application/json:
                schema:
                  type: array
                  items:
                    $ref: '#/components/schemas/Project'
        """
        username = await check_authorized(request)

        # old logic used: org_name: _Sentinel | str | None = sentinel
        org_name_str = request.query.get("org_name")
        if org_name_str is None:
            org_name: str | _Sentinel | None = sentinel
        elif org_name_str == "":
            org_name = None
        else:
            org_name = org_name_str

        name = request.query.get("name")
        cluster = request.query.get("cluster")
        owner = request.query.get("owner")
        project_name = request.query.get("project_name")

        if org_name == "":
            org_name = None
        tree_prefix = "flow://"
        if cluster is not None:
            tree_prefix += cluster
            if isinstance(org_name, str):
                tree_prefix += f"/{org_name}"
            if org_name is not sentinel and owner is not None:
                tree_prefix += f"/{owner}"
        tree = await self.auth_client.get_permissions_tree(username, tree_prefix)

        projects = (
            project
            async for project in self.storage.projects.list(
                owner=owner,
                name=name,
                cluster=cluster,
                org_name=org_name,
                project_name=project_name,
            )
            if any(tree.allows(perm) for perm in self._get_project_read_perms(project))
        )
        async with auto_close(projects):
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for project in projects:
                        payload_line = ProjectSchema().dumps(project)
                        await response.write(payload_line.encode() + b"\n")
                return response
            response_payload = [
                ProjectSchema().dump(project) async for project in projects
            ]
            return aiohttp.web.json_response(
                data=response_payload, status=HTTPOk.status_code
            )

    async def create_project(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """
        ---
        summary: Create project
        tags:
          - projects
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Project'
        responses:
          '201':
            description: Project created
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Project'
          '409':
            description: Project with such name exists
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/ClientError'
        """
        user = await untrusted_user(request)
        username = user.name
        schema = ProjectSchema()
        schema.context["username"] = username
        data = await request.json()
        data["project_name"] = data.get("project_name", username)
        project_data = schema.load(data)
        await check_permissions(
            request,
            [
                self._get_projects_write_perm(
                    project_name=project_data.project_name,
                    cluster_name=project_data.cluster,
                    org_name=project_data.org_name,
                )
            ],
        )
        try:
            project = await self.storage.projects.create(project_data)
        except ExistsError:
            return json_response(
                {
                    "code": "unique",
                    "description": "Project with such name exists",
                },
                status=HTTPConflict.status_code,
            )
        return aiohttp.web.json_response(
            data=schema.dump(project), status=HTTPCreated.status_code
        )

    async def get_project(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get project by id
        tags:
          - projects
        parameters:
          - name: id
            in: path
            required: true
            schema:
              type: string
        responses:
          '200':
            description: Found project
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Project'
          '404':
            description: Not found
        """
        proj_id = request.match_info["id"]
        project = await self._get_project(request, proj_id, write=False)
        return aiohttp.web.json_response(
            data=ProjectSchema().dump(project), status=HTTPOk.status_code
        )

    async def get_project_by_name(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """
        ---
        summary: Get project by name, cluster, org, etc.
        tags:
          - projects
        parameters:
          - name: name
            in: query
            schema:
              type: string
            required: true
          - name: cluster
            in: query
            schema:
              type: string
            required: true
          - name: owner
            in: query
            schema:
              type: string
          - name: project_name
            in: query
            schema:
              type: string
          - name: org_name
            in: query
            schema:
              type: string
        responses:
          '200':
            description: Found project
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Project'
          '404':
            description: Not found
        """
        username = await check_authorized(request)
        name = request.query["name"]
        cluster = request.query["cluster"]
        owner = request.query.get("owner")
        project_name = request.query.get("project_name")
        org_name = request.query.get("org_name")
        try:
            project = await self.storage.projects.get_by_name(
                name, project_name or owner or username, cluster, org_name
            )
        except NotExistsError:
            raise HTTPNotFound
        await self._check_project_access(request, project, write=False)
        return aiohttp.web.json_response(
            data=ProjectSchema().dump(project), status=HTTPOk.status_code
        )

    async def delete_project(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """
        ---
        summary: Delete project by id
        tags:
          - projects
        parameters:
          - name: id
            in: path
            schema:
              type: string
            required: true
        responses:
          '204':
            description: Project removed
        """
        proj_id = request.match_info["id"]
        await self._get_project(request, proj_id, write=True)
        await self.storage.projects.delete(proj_id)
        return aiohttp.web.Response(status=HTTPNoContent.status_code)


class LiveJobApiHandler(ProjectAccessMixin):
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.router.add_get("", self.list)
        app.router.add_post("", self.create)
        app.router.add_put("/replace", self.replace)
        app.router.add_get("/by_yaml_id", self.get_by_yaml_id)
        app.router.add_get("/{id}", self.get)

    @property
    def storage(self) -> Storage:
        return self._app[STORAGE]

    async def list(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.StreamResponse:
        """
        ---
        summary: List live jobs
        tags:
          - live_jobs
        parameters:
          - name: project_id
            in: query
            required: true
            schema:
              type: string
        responses:
          '200':
            description: List of live jobs
        """
        project_id = request.query["project_id"]
        try:
            await self._get_project(request, project_id, write=False)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        live_jobs = self.storage.live_jobs.list(project_id=project_id)
        async with auto_close(live_jobs):  # type: ignore[arg-type]
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for live_job in live_jobs:
                        payload_line = LiveJobSchema().dumps(live_job)
                        await response.write(payload_line.encode() + b"\n")
                return response
            response_payload = [
                LiveJobSchema().dump(live_job) async for live_job in live_jobs
            ]
            return aiohttp.web.json_response(
                data=response_payload, status=HTTPOk.status_code
            )

    async def create(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
        """
        ---
        summary: Create live job
        tags:
          - live_jobs
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/LiveJob'
        responses:
          '201':
            description: Created
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/LiveJob'
          '409':
            description: Already exists
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/ClientError'
        """
        schema = LiveJobSchema()
        live_job_data = schema.load(await request.json())
        await self._get_project(request, live_job_data.project_id, write=True)
        try:
            live_job = await self.storage.live_jobs.create(live_job_data)
        except ExistsError:
            return json_response(
                {
                    "code": "unique",
                    "description": "Live with such yaml_id exists",
                },
                status=HTTPConflict.status_code,
            )
        return aiohttp.web.json_response(
            data=schema.dump(live_job), status=HTTPCreated.status_code
        )

    async def replace(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
        """
        ---
        summary: Create or update a live job (by yaml_id)
        tags:
          - live_jobs
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/LiveJob'
        responses:
          '201':
            description: Created or replaced
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/LiveJob'
        """
        schema = LiveJobSchema()
        live_job_data = schema.load(await request.json())
        await self._get_project(request, live_job_data.project_id, write=True)
        live_job = await self.storage.live_jobs.update_or_create(live_job_data)
        return aiohttp.web.json_response(
            data=schema.dump(live_job), status=HTTPCreated.status_code
        )

    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get live job by ID
        tags:
          - live_jobs
        parameters:
          - name: id
            in: path
            schema:
              type: string
            required: true
        responses:
          '200':
            description: Found live job
          '404':
            description: Not found
        """
        job_id = request.match_info["id"]
        try:
            live_job = await self.storage.live_jobs.get(job_id)
        except NotExistsError:
            raise HTTPNotFound
        await self._get_project(request, live_job.project_id, write=False)
        return aiohttp.web.json_response(
            data=LiveJobSchema().dump(live_job), status=HTTPOk.status_code
        )

    async def get_by_yaml_id(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """
        ---
        summary: Get a live job by project_id + yaml_id
        tags:
          - live_jobs
        parameters:
          - name: project_id
            in: query
            schema:
              type: string
            required: true
          - name: yaml_id
            in: query
            schema:
              type: string
            required: true
        responses:
          '200':
            description: Found live job
          '404':
            description: Not found
        """
        project_id = request.query["project_id"]
        yaml_id = request.query["yaml_id"]
        await self._get_project(request, project_id, write=False)
        try:
            live_job = await self.storage.live_jobs.get_by_yaml_id(
                yaml_id=yaml_id,
                project_id=project_id,
            )
        except NotExistsError:
            raise HTTPNotFound
        return aiohttp.web.json_response(
            data=LiveJobSchema().dump(live_job), status=HTTPOk.status_code
        )


class BakeApiHandler(ProjectAccessMixin):
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            [
                aiohttp.web.get("", self.list),
                aiohttp.web.post("", self.create),
                aiohttp.web.get("/by_name", self.get_by_name),
                aiohttp.web.get("/{id}", self.get),
            ]
        )

    @property
    def storage(self) -> Storage:
        return self._app[STORAGE]

    async def list(self, request: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        """
        ---
        summary: List bakes
        description: Return bakes for a given project (via query), possibly NDJSON
        tags:
          - bakes
        parameters:
          - name: project_id
            in: query
            schema:
              type: string
            required: true
          - name: name
            in: query
            schema:
              type: string
          - name: tags
            in: query
            schema:
              type: array
              items:
                type: string
          - name: since
            in: query
            schema:
              type: string
              format: date-time
          - name: until
            in: query
            schema:
              type: string
              format: date-time
          - name: reverse
            in: query
            schema:
              type: boolean
          - name: fetch_last_attempt
            in: query
            required: false
            schema:
              type: string
              enum: ["1", "true"]
        responses:
          '200':
            description: A list of bakes (JSON or NDJSON)
            content:
              application/json:
                schema:
                  type: array
                  items:
                    $ref: '#/components/schemas/Bake'
        """
        project_id = request.query.get("project_id")
        if not project_id:
            raise aiohttp.web.HTTPBadRequest(text="Missing project_id")

        name = request.query.get("name")
        tags: list[str] = request.query.getall("tags", [])
        since = request.query.get("since")
        until = request.query.get("until")
        since_dt = parse_iso8601(since) if since else None
        until_dt = parse_iso8601(until) if until else None
        reverse = request.query.get("reverse") in ("1", "true")
        fetch_last_attempt = request.query.get("fetch_last_attempt") in ("1", "true")

        try:
            await self._get_project(request, project_id, write=False)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        bakes = self.storage.bakes.list(
            project_id=project_id,
            name=name,
            tags=set(tags),
            since=since_dt,
            until=until_dt,
            reverse=reverse,
            fetch_last_attempt=fetch_last_attempt,
        )
        async with auto_close(bakes):  # type: ignore[arg-type]
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for bake in bakes:
                        payload_line = BakeSchema().dumps(bake)
                        await response.write(payload_line.encode() + b"\n")
                return response
            response_payload = [BakeSchema().dump(bake) async for bake in bakes]
            return aiohttp.web.json_response(
                data=response_payload, status=HTTPOk.status_code
            )

    async def create(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Create a new bake
        tags:
          - bakes
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Bake'
        responses:
          '201':
            description: Bake created
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Bake'
          '409':
            description: Bake with such ID exists
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/ClientError'
        """
        body = await request.json()
        # parse partial fields manually or with BakeSchema, e.g.:
        schema = BakeSchema(partial=["name", "tags"])
        bake_data = schema.load(body)

        await self._get_project(request, bake_data.project_id, write=True)
        try:
            bake = await self.storage.bakes.create(bake_data)
        except ExistsError:
            return json_response(
                {
                    "code": "unique",
                    "description": "Bake with such id exists",
                },
                status=HTTPConflict.status_code,
            )
        return aiohttp.web.json_response(
            data=schema.dump(bake), status=HTTPCreated.status_code
        )

    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get bake by ID
        tags:
          - bakes
        parameters:
          - name: id
            in: path
            schema:
              type: string
            required: true
          - name: fetch_last_attempt
            in: query
            required: false
            schema:
              type: string
              enum: ["1", "true"]
        responses:
          '200':
            description: The bake object
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Bake'
          '404':
            description: Not found
        """
        fetch_last_attempt = request.query.get("fetch_last_attempt") in ("1", "true")
        id = request.match_info["id"]
        try:
            bake = await self.storage.bakes.get(
                id, fetch_last_attempt=fetch_last_attempt
            )
        except NotExistsError:
            raise HTTPNotFound
        await self._get_project(request, bake.project_id, write=False)
        return aiohttp.web.json_response(
            data=BakeSchema().dump(bake), status=HTTPOk.status_code
        )

    async def get_by_name(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get bake by name
        tags:
          - bakes
        parameters:
          - name: project_id
            in: query
            schema:
              type: string
            required: true
          - name: name
            in: query
            schema:
              type: string
            required: true
          - name: fetch_last_attempt
            in: query
            required: false
            schema:
              type: string
              enum: ["1", "true"]
        responses:
          '200':
            description: The bake object
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Bake'
          '404':
            description: Not found
        """
        project_id = request.query.get("project_id")
        if not project_id:
            raise HTTPBadRequest(text="Missing project_id")
        name = request.query.get("name")
        if not name:
            raise HTTPBadRequest(text="Missing name")
        fetch_last_attempt = request.query.get("fetch_last_attempt") in ("1", "true")

        try:
            bake = await self.storage.bakes.get_by_name(
                project_id=project_id, name=name, fetch_last_attempt=fetch_last_attempt
            )
        except NotExistsError:
            raise HTTPNotFound
        await self._get_project(request, bake.project_id, write=False)
        return aiohttp.web.json_response(
            data=BakeSchema().dump(bake), status=HTTPOk.status_code
        )


class AttemptApiHandler(ProjectAccessMixin):
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            [
                aiohttp.web.get("", self.list),
                aiohttp.web.post("", self.create),
                aiohttp.web.get("/by_number", self.get_by_number),
                aiohttp.web.put("/replace", self.replace),
                aiohttp.web.get("/{id}", self.get),
            ]
        )

    @property
    def storage(self) -> Storage:
        return self._app[STORAGE]

    async def _get_bake(self, bake_id: str) -> Bake:
        try:
            return await self.storage.bakes.get(bake_id)
        except NotExistsError:
            raise HTTPNotFound

    async def list(self, request: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        """
        ---
        summary: List attempts in a bake
        tags:
          - attempts
        parameters:
          - name: bake_id
            in: query
            required: true
            schema:
              type: string
        responses:
          '200':
            description: A list of attempts
        """
        bake_id = request.query.get("bake_id")
        if not bake_id:
            raise HTTPBadRequest(text="Missing bake_id")

        try:
            bake = await self._get_bake(bake_id)
            await self._get_project(request, bake.project_id, write=False)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        attempts = self.storage.attempts.list(bake_id=bake_id)
        async with auto_close(attempts):  # type: ignore[arg-type]
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for attempt in attempts:
                        payload_line = AttemptSchema().dumps(attempt)
                        await response.write(payload_line.encode() + b"\n")
                return response
            response_payload = [
                AttemptSchema().dump(attempt) async for attempt in attempts
            ]
            return aiohttp.web.json_response(
                data=response_payload, status=HTTPOk.status_code
            )

    async def create(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Create new attempt
        tags:
          - attempts
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Attempt'
        responses:
          '201':
            description: Attempt created
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Attempt'
          '409':
            description: Attempt with such bake+number exists
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/ClientError'
        """
        schema = AttemptSchema(partial=["executor_id"])
        attempt_data = schema.load(await request.json())
        bake = await self._get_bake(attempt_data.bake_id)
        await self._get_project(request, bake.project_id, write=True)
        try:
            attempt = await self.storage.attempts.create(attempt_data)
        except ExistsError:
            return json_response(
                {
                    "code": "unique",
                    "description": "Attempt with such bake and number exists",
                },
                status=HTTPConflict.status_code,
            )
        return aiohttp.web.json_response(
            data=schema.dump(attempt), status=HTTPCreated.status_code
        )

    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get attempt by ID
        tags:
          - attempts
        parameters:
          - name: id
            in: path
            schema:
              type: string
            required: true
        responses:
          '200':
            description: The attempt
          '404':
            description: Not found
        """
        id = request.match_info["id"]
        try:
            attempt = await self.storage.attempts.get(id)
        except NotExistsError:
            raise HTTPNotFound
        bake = await self._get_bake(attempt.bake_id)
        await self._get_project(request, bake.project_id, write=False)
        return aiohttp.web.json_response(
            data=AttemptSchema().dump(attempt), status=HTTPOk.status_code
        )

    async def get_by_number(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get attempt by bake & attempt number
        tags:
          - attempts
        parameters:
          - name: bake_id
            in: query
            schema:
              type: string
            required: true
          - name: number
            in: query
            schema:
              type: integer
            required: true
        responses:
          '200':
            description: The attempt
          '404':
            description: Not found
        """
        bake_id = request.query.get("bake_id")
        if not bake_id:
            raise HTTPBadRequest(text="Missing bake_id")
        number_str = request.query.get("number")
        if not number_str:
            raise HTTPBadRequest(text="Missing number")
        number = int(number_str)

        bake = await self._get_bake(bake_id)
        await self._get_project(request, bake.project_id, write=False)
        try:
            attempt = await self.storage.attempts.get_by_number(
                bake_id=bake_id,
                number=number,
            )
        except NotExistsError:
            raise HTTPNotFound
        return aiohttp.web.json_response(
            data=AttemptSchema().dump(attempt), status=HTTPOk.status_code
        )

    async def replace(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Update existing attempt
        tags:
          - attempts
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Attempt'
        responses:
          '200':
            description: Attempt replaced
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/Attempt'
        """
        schema = AttemptSchema(partial=["executor_id"])
        attempt_data = schema.load(await request.json())
        attempt = await self.storage.attempts.get_by_number(
            attempt_data.bake_id, attempt_data.number
        )
        bake = await self._get_bake(attempt_data.bake_id)
        await self._get_project(request, bake.project_id, write=True)
        new_attempt = replace(
            attempt, result=attempt_data.result, executor_id=attempt_data.executor_id
        )
        await self.storage.attempts.update(new_attempt)
        return aiohttp.web.json_response(
            data=schema.dump(new_attempt), status=HTTPOk.status_code
        )


class TaskApiHandler(ProjectAccessMixin):
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.router.add_get("", self.list)
        app.router.add_post("", self.create)
        app.router.add_put("/replace", self.replace)
        app.router.add_get("/by_yaml_id", self.get_by_yaml_id)
        app.router.add_get("/{id}", self.get)

    @property
    def storage(self) -> Storage:
        return self._app[STORAGE]

    async def _get_bake(self, bake_id: str) -> Bake:
        try:
            return await self.storage.bakes.get(bake_id)
        except NotExistsError:
            raise HTTPNotFound

    async def _get_attempt(self, attempt_id: str) -> Attempt:
        try:
            return await self.storage.attempts.get(attempt_id)
        except NotExistsError:
            raise HTTPNotFound

    async def list(self, request: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        """
        ---
        summary: List tasks in an attempt
        tags:
          - tasks
        parameters:
          - name: attempt_id
            in: query
            schema:
              type: string
            required: true
        responses:
          '200':
            description: list of tasks
        """
        attempt_id = request.query.get("attempt_id")
        if not attempt_id:
            raise HTTPBadRequest(text="Missing attempt_id")

        try:
            attempt = await self._get_attempt(attempt_id)
            bake = await self._get_bake(attempt.bake_id)
            await self._get_project(request, bake.project_id, write=False)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        tasks = self.storage.tasks.list(attempt_id=attempt_id)
        async with auto_close(tasks):  # type: ignore[arg-type]
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for task in tasks:
                        payload_line = TaskSchema().dumps(task)
                        await response.write(payload_line.encode() + b"\n")
                return response
            response_payload = [TaskSchema().dump(task) async for task in tasks]
            return aiohttp.web.json_response(
                data=response_payload, status=HTTPOk.status_code
            )

    async def create(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Create a new task
        tags:
          - tasks
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Task'
        responses:
          '201':
            description: Task created
          '409':
            description: Task already exists
        """
        schema = TaskSchema()
        task_data = schema.load(await request.json())
        attempt = await self._get_attempt(task_data.attempt_id)
        bake = await self._get_bake(attempt.bake_id)
        await self._get_project(request, bake.project_id, write=True)
        try:
            task = await self.storage.tasks.create(task_data)
        except ExistsError:
            return json_response(
                {
                    "code": "unique",
                    "description": "Task already exists",
                },
                status=HTTPConflict.status_code,
            )
        return aiohttp.web.json_response(
            data=schema.dump(task), status=HTTPCreated.status_code
        )

    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get a task by ID
        tags:
          - tasks
        parameters:
          - name: id
            in: path
            schema:
              type: string
            required: true
        responses:
          '200':
            description: The task
          '404':
            description: Not found
        """
        id = request.match_info["id"]
        try:
            task = await self.storage.tasks.get(id)
        except NotExistsError:
            raise HTTPNotFound
        attempt = await self._get_attempt(task.attempt_id)
        bake = await self._get_bake(attempt.bake_id)
        await self._get_project(request, bake.project_id, write=False)
        return aiohttp.web.json_response(
            data=TaskSchema().dump(task), status=HTTPOk.status_code
        )

    async def get_by_yaml_id(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        """
        ---
        summary: Get a task by attempt + yaml_id
        tags:
          - tasks
        parameters:
          - name: attempt_id
            in: query
            schema:
              type: string
            required: true
          - name: yaml_id
            in: query
            schema:
              type: string
            required: true
        responses:
          '200':
            description: The task
          '404':
            description: Not found
        """
        attempt_id = request.query.get("attempt_id")
        yaml_id = request.query.get("yaml_id")
        if not attempt_id or not yaml_id:
            raise HTTPBadRequest(text="Missing attempt_id or yaml_id")

        attempt = await self._get_attempt(attempt_id)
        bake = await self._get_bake(attempt.bake_id)
        await self._get_project(request, bake.project_id, write=False)
        try:
            task = await self.storage.tasks.get_by_yaml_id(
                yaml_id=tuple(yaml_id.split(".")),
                attempt_id=attempt_id,
            )
        except NotExistsError:
            raise HTTPNotFound
        return aiohttp.web.json_response(
            data=TaskSchema().dump(task), status=HTTPOk.status_code
        )

    async def replace(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Update task
        tags:
          - tasks
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Task'
        responses:
          '200':
            description: Task replaced
        """
        schema = TaskSchema()
        task_data = schema.load(await request.json())
        task = await self.storage.tasks.get_by_yaml_id(
            task_data.yaml_id, task_data.attempt_id
        )
        attempt = await self._get_attempt(task_data.attempt_id)
        bake = await self._get_bake(attempt.bake_id)
        await self._get_project(request, bake.project_id, write=True)

        new_task = replace(
            task,
            raw_id=task_data.raw_id,
            outputs=task_data.outputs,
            state=task_data.state,
            statuses=task_data.statuses,
        )
        await self.storage.tasks.update(new_task)
        return aiohttp.web.json_response(
            data=schema.dump(new_task), status=HTTPOk.status_code
        )


class ConfigFileApiHandler(ProjectAccessMixin):
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.router.add_post("", self.create)
        app.router.add_get("/{id}", self.get)

    @property
    def storage(self) -> Storage:
        return self._app[STORAGE]

    async def _get_bake(self, bake_id: str) -> Bake:
        try:
            return await self.storage.bakes.get(bake_id)
        except NotExistsError:
            raise HTTPNotFound

    async def create(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Create a config file
        tags:
          - config_files
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ConfigFile'
        responses:
          '201':
            description: Config file created
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/ConfigFile'
          '409':
            description: Config file already exists
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/ClientError'
        """
        schema = ConfigFileSchema()
        config_file_data = schema.load(await request.json())
        bake = await self._get_bake(config_file_data.bake_id)
        await self._get_project(request, bake.project_id, write=True)
        try:
            config_file = await self.storage.config_files.create(config_file_data)
        except ExistsError:
            return json_response(
                {
                    "code": "unique",
                    "description": "Config file exists",
                },
                status=HTTPConflict.status_code,
            )
        return aiohttp.web.json_response(
            data=schema.dump(config_file), status=HTTPCreated.status_code
        )

    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get a config file by ID
        tags:
          - config_files
        parameters:
          - name: id
            in: path
            schema:
              type: string
            required: true
        responses:
          '200':
            description: Found config file
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/ConfigFile'
          '404':
            description: Not found
        """
        id = request.match_info["id"]
        try:
            config_file = await self.storage.config_files.get(id)
        except NotExistsError:
            raise HTTPNotFound
        bake = await self._get_bake(config_file.bake_id)
        await self._get_project(request, bake.project_id, write=False)
        return aiohttp.web.json_response(
            data=ConfigFileSchema().dump(config_file), status=HTTPOk.status_code
        )


class CacheEntryApiHandler(ProjectAccessMixin):
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.router.add_post("", self.create)
        app.router.add_delete("", self.delete)
        app.router.add_get("/by_key", self.get_by_key)
        app.router.add_get("/{id}", self.get)

    @property
    def storage(self) -> Storage:
        return self._app[STORAGE]

    async def create(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Create cache entry
        tags:
          - cache_entries
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/CacheEntry'
        responses:
          '201':
            description: Cache entry created
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/CacheEntry'
        """
        schema = CacheEntrySchema(partial=["raw_id"])
        data = schema.load(await request.json())
        await self._get_project(request, data.project_id, write=True)
        try:
            cache_entry = await self.storage.cache_entries.create(data)
        except ExistsError:
            old_entry = await self.storage.cache_entries.get_by_key(
                data.project_id, data.task_id, data.batch, data.key
            )
            cache_entry = replace(
                old_entry,
                created_at=data.created_at,
                outputs=data.outputs,
                state=data.state,
            )
            await self.storage.cache_entries.update(cache_entry)
        return aiohttp.web.json_response(
            data=CacheEntrySchema().dump(cache_entry), status=HTTPCreated.status_code
        )

    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get a cache entry by ID
        tags:
          - cache_entries
        parameters:
          - name: id
            in: path
            schema:
              type: string
            required: true
        responses:
          '200':
            description: Found cache entry
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/CacheEntry'
          '404':
            description: Not found
        """
        id = request.match_info["id"]
        try:
            cache_entry = await self.storage.cache_entries.get(id)
        except NotExistsError:
            raise HTTPNotFound
        await self._get_project(request, cache_entry.project_id, write=False)
        return aiohttp.web.json_response(
            data=CacheEntrySchema().dump(cache_entry), status=HTTPOk.status_code
        )

    async def get_by_key(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get a cache entry by (project_id, task_id, batch, key)
        tags:
          - cache_entries
        parameters:
          - name: project_id
            in: query
            schema:
              type: string
            required: true
          - name: task_id
            in: query
            schema:
              type: string
            required: true
          - name: batch
            in: query
            schema:
              type: string
            required: true
          - name: key
            in: query
            schema:
              type: string
            required: true
        responses:
          '200':
            description: Found cache entry
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/CacheEntry'
          '404':
            description: Not found
        """
        project_id = request.query.get("project_id")
        task_id = request.query.get("task_id")
        batch = request.query.get("batch")
        key = request.query.get("key")
        if not project_id or not task_id or not batch or not key:
            raise HTTPBadRequest(text="Missing some required query param")

        await self._get_project(request, project_id, write=False)
        try:
            cache_entry = await self.storage.cache_entries.get_by_key(
                project_id=project_id,
                task_id=tuple(task_id.split(".")),
                batch=batch,
                key=key,
            )
        except NotExistsError:
            raise HTTPNotFound
        return aiohttp.web.json_response(
            data=CacheEntrySchema().dump(cache_entry), status=HTTPOk.status_code
        )

    async def delete(self, request: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        """
        ---
        summary: Clear multiple cache entries
        tags:
          - cache_entries
        parameters:
          - name: project_id
            in: query
            schema:
              type: string
            required: true
          - name: task_id
            in: query
            schema:
              type: string
          - name: batch
            in: query
            schema:
              type: string
        responses:
          '204':
            description: Entries cleared
        """
        project_id = request.query.get("project_id")
        if not project_id:
            raise HTTPBadRequest(text="Missing project_id")

        task_id = request.query.get("task_id")
        batch = request.query.get("batch")

        await self._get_project(request, project_id, write=True)
        await self.storage.cache_entries.delete_all(
            project_id=project_id,
            batch=batch,
            task_id=tuple(task_id.split(".")) if task_id else None,
        )
        return aiohttp.web.Response(status=HTTPNoContent.status_code)


class BakeImagesApiHandler(ProjectAccessMixin):
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.router.add_get("", self.list)
        app.router.add_post("", self.create)
        app.router.add_get("/by_ref", self.get_by_ref)
        app.router.add_patch("/{id}", self.patch)
        app.router.add_get("/{id}", self.get)

    @property
    def storage(self) -> Storage:
        return self._app[STORAGE]

    async def _get_bake(self, bake_id: str) -> Bake:
        try:
            return await self.storage.bakes.get(bake_id)
        except NotExistsError:
            raise HTTPNotFound

    async def list(self, request: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        """
        ---
        summary: List bake images
        tags:
          - bake_images
        parameters:
          - name: bake_id
            in: query
            schema:
              type: string
            required: true
        responses:
          '200':
            description: List of bake images
        """
        bake_id = request.query.get("bake_id")
        if not bake_id:
            raise HTTPBadRequest(text="Missing bake_id")

        try:
            bake = await self._get_bake(bake_id)
            await self._get_project(request, bake.project_id, write=False)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        bake_images = self.storage.bake_images.list(bake_id=bake_id)
        async with auto_close(bake_images):  # type: ignore[arg-type]
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for image in bake_images:
                        payload_line = BakeImageSchema().dumps(image)
                        await response.write(payload_line.encode() + b"\n")
                return response
            response_payload = [
                BakeImageSchema().dump(image) async for image in bake_images
            ]
            return aiohttp.web.json_response(
                data=response_payload, status=HTTPOk.status_code
            )

    async def create(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Create a bake image
        tags:
          - bake_images
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/BakeImage'
        responses:
          '201':
            description: Bake image created
          '409':
            description: Bake image with such bake+ref exists
        """
        schema = BakeImageSchema(
            partial=["context_on_storage", "dockerfile_rel", "builder_job_id"]
        )
        image_data = schema.load(await request.json())
        bake = await self._get_bake(image_data.bake_id)
        await self._get_project(request, bake.project_id, write=True)
        try:
            image = await self.storage.bake_images.create(image_data)
        except ExistsError:
            return json_response(
                {
                    "code": "unique",
                    "description": "Bake image with such bake and ref exists",
                },
                status=HTTPConflict.status_code,
            )
        return aiohttp.web.json_response(
            data=schema.dump(image), status=HTTPCreated.status_code
        )

    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get bake image by ID
        tags:
          - bake_images
        parameters:
          - name: id
            in: path
            schema:
              type: string
            required: true
        responses:
          '200':
            description: The bake image
          '404':
            description: Not found
        """
        id = request.match_info["id"]
        try:
            image = await self.storage.bake_images.get(id)
        except NotExistsError:
            raise HTTPNotFound
        bake = await self._get_bake(image.bake_id)
        await self._get_project(request, bake.project_id, write=False)
        return aiohttp.web.json_response(
            data=BakeImageSchema().dump(image), status=HTTPOk.status_code
        )

    async def get_by_ref(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Get bake image by bake_id + ref
        tags:
          - bake_images
        parameters:
          - name: bake_id
            in: query
            schema:
              type: string
            required: true
          - name: ref
            in: query
            schema:
              type: string
            required: true
        responses:
          '200':
            description: The bake image
          '404':
            description: Not found
        """
        bake_id = request.query.get("bake_id")
        if not bake_id:
            raise HTTPBadRequest(text="Missing bake_id")
        ref = request.query.get("ref")
        if not ref:
            raise HTTPBadRequest(text="Missing ref")

        bake = await self._get_bake(bake_id)
        await self._get_project(request, bake.project_id, write=False)
        try:
            image = await self.storage.bake_images.get_by_ref(
                bake_id=bake_id,
                ref=ref,
            )
        except NotExistsError:
            raise HTTPNotFound
        return aiohttp.web.json_response(
            data=BakeImageSchema().dump(image), status=HTTPOk.status_code
        )

    async def patch(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        ---
        summary: Patch an existing bake image
        tags:
          - bake_images
        parameters:
          - name: id
            in: path
            schema:
              type: string
            required: true
        requestBody:
          required: true
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/BakeImagePatch'
        responses:
          '200':
            description: Bake image patched
            content:
              application/json:
                schema:
                  $ref: '#/components/schemas/BakeImage'
          '404':
            description: Not found
        """
        id = request.match_info["id"]
        try:
            image = await self.storage.bake_images.get(id)
        except NotExistsError:
            raise HTTPNotFound
        bake = await self._get_bake(image.bake_id)
        await self._get_project(request, bake.project_id, write=True)
        schema = BakeImagePatchSchema()
        new_values = schema.load(await request.json())
        new_image = replace(image, **new_values)
        await self.storage.bake_images.update(new_image)
        return aiohttp.web.json_response(
            data=BakeImageSchema().dump(new_image), status=HTTPOk.status_code
        )


@middleware
async def handle_exceptions(
    request: Request, handler: Callable[[Request], Awaitable[StreamResponse]]
) -> StreamResponse:
    try:
        return await handler(request)
    except (ValueError, ValidationError) as e:
        return web.json_response({"error": str(e)}, status=HTTPBadRequest.status_code)
    except aiohttp.web.HTTPException as e:
        msg = str(e)
        if "authorization" in msg.lower():
            code = HTTPUnauthorized.status_code
        elif "required property" in msg.lower():
            code = HTTPUnprocessableEntity.status_code
        else:
            code = e.status

        try:
            msg = json.loads(msg)
        except Exception:
            msg = str(e)
        return web.json_response({"error": msg}, status=code)
    except Exception as e:
        msg_str = f"Unexpected exception: {str(e)}. Path with query: {request.path_qs}."
        logging.exception(msg_str)
        payload = {"error": msg_str}
        return json_response(payload, status=HTTPInternalServerError.status_code)


async def create_projects_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = ProjectsApiHandler(app, config)
    handler.register(app)
    app[HANDLER] = handler
    return app


async def create_live_jobs_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = LiveJobApiHandler(app, config)
    handler.register(app)
    app[HANDLER] = handler
    return app


async def create_bakes_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = BakeApiHandler(app, config)
    handler.register(app)
    app[HANDLER] = handler
    return app


async def create_attempts_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = AttemptApiHandler(app, config)
    handler.register(app)
    app[HANDLER] = handler
    return app


async def create_tasks_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = TaskApiHandler(app, config)
    handler.register(app)
    app[HANDLER] = handler
    return app


async def create_config_files_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = ConfigFileApiHandler(app, config)
    handler.register(app)
    app[HANDLER] = handler
    return app


async def create_cache_entries_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = CacheEntryApiHandler(app, config)
    handler.register(app)
    app[HANDLER] = handler
    return app


async def create_bake_images_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = BakeImagesApiHandler(app, config)
    handler.register(app)
    app[HANDLER] = handler
    return app


@asynccontextmanager
async def create_auth_client(config: PlatformAuthConfig) -> AsyncIterator[AuthClient]:
    async with AuthClient(config.url, config.token) as client:
        yield client


async def add_version_to_header(request: Request, response: StreamResponse) -> None:
    response.headers["X-Service-Version"] = f"{APP_NAME}/{__version__}"


async def create_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app[CONFIG] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing Auth client")
            auth_client = await exit_stack.enter_async_context(
                create_auth_client(config.platform_auth)
            )

            logger.info("Initializing SQLAlchemy engine")
            engine = make_async_engine(config.postgres)
            exit_stack.push_async_callback(engine.dispose)

            logger.info("Initializing PostgresStorage")
            storage: Storage = PostgresStorage(engine)

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

            logger.info("Initializing Platform Api client")
            platform_client = await exit_stack.enter_async_context(
                PlatformApiClient(
                    url=config.platform_api.url, token=config.platform_api.token
                )
            )

            await exit_stack.enter_async_context(
                WatchersPoller(
                    interval_sec=config.watchers.polling_interval_sec,
                    watchers=[
                        ExecutorAliveWatcher(storage.attempts, platform_client),
                    ],
                )
            )

            await exit_stack.enter_async_context(
                ProjectDeleter(storage, config.kube, config.events)
            )

            app[PROJECTS_APP][STORAGE] = storage
            app[PROJECTS_APP][AUTH_CLIENT] = auth_client
            app[LIVE_JOBS_APP][STORAGE] = storage
            app[BAKES_APP][STORAGE] = storage
            app[ATTEMPTS_APP][STORAGE] = storage
            app[TASKS_APP][STORAGE] = storage
            app[CACHE_ENTRIES_APP][STORAGE] = storage
            app[CONFIG_FILES_APP][STORAGE] = storage
            app[BAKE_IMAGES_APP][STORAGE] = storage

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = aiohttp.web.Application()
    api_handler = ApiHandler()
    api_handler.register(api_v1_app)
    api_v1_app[HANDLER] = api_handler

    projects_app = await create_projects_app(config)
    live_jobs_app = await create_live_jobs_app(config)
    bakes_app = await create_bakes_app(config)
    attempts_app = await create_attempts_app(config)
    tasks_app = await create_tasks_app(config)
    config_files_app = await create_config_files_app(config)
    cache_entries_app = await create_cache_entries_app(config)
    bake_images_app = await create_bake_images_app(config)

    # Store references to each subapp
    app[PROJECTS_APP] = projects_app
    app[LIVE_JOBS_APP] = live_jobs_app
    app[BAKES_APP] = bakes_app
    app[ATTEMPTS_APP] = attempts_app
    app[TASKS_APP] = tasks_app
    app[CONFIG_FILES_APP] = config_files_app
    app[CACHE_ENTRIES_APP] = cache_entries_app
    app[BAKE_IMAGES_APP] = bake_images_app

    # Mount them under /api/v1/flow/<something>
    api_v1_app.add_subapp("/flow/projects", projects_app)
    api_v1_app.add_subapp("/flow/live_jobs", live_jobs_app)
    api_v1_app.add_subapp("/flow/bakes", bakes_app)
    api_v1_app.add_subapp("/flow/attempts", attempts_app)
    api_v1_app.add_subapp("/flow/tasks", tasks_app)
    api_v1_app.add_subapp("/flow/config_files", config_files_app)
    api_v1_app.add_subapp("/flow/cache_entries", cache_entries_app)
    api_v1_app.add_subapp("/flow/bake_images", bake_images_app)

    app.add_subapp("/api/v1", api_v1_app)

    async def handle_ping(request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response(text="Pong")

    app.router.add_get("/ping", handle_ping)

    # Add version header to each response
    app.on_response_prepare.append(add_version_to_header)

    if config.enable_docs:
        prefix = "/api/docs/v1/flow"
        from aiohttp_swagger3 import SwaggerDocs, SwaggerInfo, SwaggerUiSettings

        docs = SwaggerDocs(
            app,
            info=SwaggerInfo(
                title="Apolo Flow API documentation",
                version="v1",
                description="API for managing flow entities",
            ),
            swagger_ui_settings=SwaggerUiSettings(path=f"{prefix}/ui"),
        )
        docs.spec["components"] = {
            "securitySchemes": {
                "jwt": {
                    "type": "apiKey",
                    "name": "Authorization",
                    "in": "header",
                }
            },
            "schemas": {
                "Project": {
                    "type": "object",
                    "required": ["id", "name", "owner", "project_name", "cluster"],
                    "properties": {
                        "id": {"type": "string", "readOnly": True},
                        "name": {"type": "string"},
                        "owner": {"type": "string", "readOnly": True},
                        "project_name": {"type": "string"},
                        "cluster": {"type": "string"},
                        "org_name": {"type": "string", "nullable": True},
                    },
                },
                "LiveJob": {
                    "type": "object",
                    "required": ["id", "yaml_id", "project_id", "multi", "tags"],
                    "properties": {
                        "id": {"type": "string", "readOnly": True},
                        "yaml_id": {"type": "string"},
                        "project_id": {"type": "string"},
                        "multi": {"type": "boolean"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "raw_id": {"type": "string"},
                    },
                },
                "Bake": {
                    "type": "object",
                    "required": [
                        "id",
                        "project_id",
                        "batch",
                        "graphs",
                        "params",
                        "tags",
                    ],
                    "properties": {
                        "id": {"type": "string", "readOnly": True},
                        "project_id": {"type": "string"},
                        "batch": {"type": "string"},
                        "created_at": {"type": "string", "format": "date-time"},
                        "meta": {"type": "object"},
                        "graphs": {"type": "object"},
                        "params": {"type": "object"},
                        "name": {"type": "string", "nullable": True},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "last_attempt": {
                            "$ref": "#/components/schemas/Attempt",
                            "readOnly": True,
                        },
                    },
                },
                "Attempt": {
                    "type": "object",
                    "required": ["id", "bake_id", "number", "result", "configs_meta"],
                    "properties": {
                        "id": {"type": "string", "readOnly": True},
                        "bake_id": {"type": "string"},
                        "number": {"type": "integer"},
                        "created_at": {"type": "string", "format": "date-time"},
                        "result": {"type": "string"},
                        "configs_meta": {"type": "object"},
                        "executor_id": {"type": "string", "nullable": True},
                    },
                },
                "Task": {
                    "type": "object",
                    "required": ["yaml_id", "attempt_id", "statuses"],
                    "properties": {
                        "id": {"type": "string", "readOnly": True},
                        "yaml_id": {"type": "string"},
                        "attempt_id": {"type": "string"},
                        "raw_id": {"type": "string"},
                        "outputs": {"type": "object", "nullable": True},
                        "state": {"type": "object", "nullable": True},
                        "statuses": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/TaskStatusItem"},
                        },
                    },
                },
                "TaskStatusItem": {
                    "type": "object",
                    "required": ["created_at", "status"],
                    "properties": {
                        "created_at": {"type": "string", "format": "date-time"},
                        "status": {"type": "string"},
                    },
                },
                "ConfigFile": {
                    "type": "object",
                    "required": ["id", "bake_id", "filename", "content"],
                    "properties": {
                        "id": {"type": "string", "readOnly": True},
                        "bake_id": {"type": "string"},
                        "filename": {"type": "string"},
                        "content": {"type": "string"},
                    },
                },
                "CacheEntry": {
                    "type": "object",
                    "required": ["id", "project_id", "task_id", "batch", "key"],
                    "properties": {
                        "id": {"type": "string", "readOnly": True},
                        "project_id": {"type": "string"},
                        "task_id": {"type": "string"},
                        "batch": {"type": "string"},
                        "key": {"type": "string"},
                        "created_at": {"type": "string", "format": "date-time"},
                        "raw_id": {"type": "string"},
                        "outputs": {"type": "object"},
                        "state": {"type": "object"},
                    },
                },
                "BakeImage": {
                    "type": "object",
                    "required": ["id", "bake_id", "ref", "status", "yaml_defs"],
                    "properties": {
                        "id": {"type": "string", "readOnly": True},
                        "bake_id": {"type": "string"},
                        "ref": {"type": "string"},
                        "status": {"type": "string"},
                        "yaml_defs": {"type": "array", "items": {"type": "string"}},
                        "context_on_storage": {"type": "string", "nullable": True},
                        "dockerfile_rel": {"type": "string", "nullable": True},
                        "builder_job_id": {"type": "string", "nullable": True},
                        "prefix": {"type": "string"},
                        "yaml_id": {"type": "string"},
                    },
                },
                "BakeImagePatch": {
                    "type": "object",
                    "properties": {
                        "status": {"type": "string"},
                        "builder_job_id": {"type": "string"},
                    },
                },
                "ClientError": {
                    "type": "object",
                    "required": ["code", "description"],
                    "properties": {
                        "code": {"type": "string"},
                        "description": {"type": "string"},
                    },
                },
            },
        }
        docs.add_routes(
            [
                # 1) Top-level pings from ApiHandler
                get("/api/v1/ping", api_v1_app[HANDLER].handle_ping),
                get("/api/v1/secured-ping", api_v1_app[HANDLER].handle_secured_ping),
                # 2) ProjectsApiHandler
                get("/api/v1/flow/projects", app[PROJECTS_APP][HANDLER].list_projects),
                post(
                    "/api/v1/flow/projects", app[PROJECTS_APP][HANDLER].create_project
                ),
                get(
                    "/api/v1/flow/projects/by_name",
                    app[PROJECTS_APP][HANDLER].get_project_by_name,
                ),
                get(
                    "/api/v1/flow/projects/{id}", app[PROJECTS_APP][HANDLER].get_project
                ),
                delete(
                    "/api/v1/flow/projects/{id}",
                    app[PROJECTS_APP][HANDLER].delete_project,
                ),
                # 3) LiveJobApiHandler
                get("/api/v1/flow/live_jobs", app[LIVE_JOBS_APP][HANDLER].list),
                post("/api/v1/flow/live_jobs", app[LIVE_JOBS_APP][HANDLER].create),
                put(
                    "/api/v1/flow/live_jobs/replace",
                    app[LIVE_JOBS_APP][HANDLER].replace,
                ),
                get(
                    "/api/v1/flow/live_jobs/by_yaml_id",
                    app[LIVE_JOBS_APP][HANDLER].get_by_yaml_id,
                ),
                get("/api/v1/flow/live_jobs/{id}", app[LIVE_JOBS_APP][HANDLER].get),
                # 4) BakeApiHandler
                get("/api/v1/flow/bakes", app[BAKES_APP][HANDLER].list),
                post("/api/v1/flow/bakes", app[BAKES_APP][HANDLER].create),
                get("/api/v1/flow/bakes/by_name", app[BAKES_APP][HANDLER].get_by_name),
                get("/api/v1/flow/bakes/{id}", app[BAKES_APP][HANDLER].get),
                # 5) AttemptApiHandler
                get("/api/v1/flow/attempts", app[ATTEMPTS_APP][HANDLER].list),
                post("/api/v1/flow/attempts", app[ATTEMPTS_APP][HANDLER].create),
                get(
                    "/api/v1/flow/attempts/by_number",
                    app[ATTEMPTS_APP][HANDLER].get_by_number,
                ),
                put(
                    "/api/v1/flow/attempts/replace", app[ATTEMPTS_APP][HANDLER].replace
                ),
                get("/api/v1/flow/attempts/{id}", app[ATTEMPTS_APP][HANDLER].get),
                # 6) TaskApiHandler
                get("/api/v1/flow/tasks", app[TASKS_APP][HANDLER].list),
                post("/api/v1/flow/tasks", app[TASKS_APP][HANDLER].create),
                put("/api/v1/flow/tasks/replace", app[TASKS_APP][HANDLER].replace),
                get(
                    "/api/v1/flow/tasks/by_yaml_id",
                    app[TASKS_APP][HANDLER].get_by_yaml_id,
                ),
                get("/api/v1/flow/tasks/{id}", app[TASKS_APP][HANDLER].get),
                # 7) ConfigFileApiHandler
                post(
                    "/api/v1/flow/config_files", app[CONFIG_FILES_APP][HANDLER].create
                ),
                get(
                    "/api/v1/flow/config_files/{id}", app[CONFIG_FILES_APP][HANDLER].get
                ),
                # 8) CacheEntryApiHandler
                post(
                    "/api/v1/flow/cache_entries", app[CACHE_ENTRIES_APP][HANDLER].create
                ),
                delete(
                    "/api/v1/flow/cache_entries", app[CACHE_ENTRIES_APP][HANDLER].delete
                ),
                get(
                    "/api/v1/flow/cache_entries/by_key",
                    app[CACHE_ENTRIES_APP][HANDLER].get_by_key,
                ),
                get(
                    "/api/v1/flow/cache_entries/{id}",
                    app[CACHE_ENTRIES_APP][HANDLER].get,
                ),
                # 9) BakeImagesApiHandler
                get("/api/v1/flow/bake_images", app[BAKE_IMAGES_APP][HANDLER].list),
                post("/api/v1/flow/bake_images", app[BAKE_IMAGES_APP][HANDLER].create),
                get(
                    "/api/v1/flow/bake_images/by_ref",
                    app[BAKE_IMAGES_APP][HANDLER].get_by_ref,
                ),
                patch(
                    "/api/v1/flow/bake_images/{id}", app[BAKE_IMAGES_APP][HANDLER].patch
                ),
                get("/api/v1/flow/bake_images/{id}", app[BAKE_IMAGES_APP][HANDLER].get),
            ]
        )

    return app


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    setup_sentry()
    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )
