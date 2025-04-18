from __future__ import annotations

import abc
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from datetime import datetime

import aiohttp
import aiohttp.web
from aiohttp.web import (
    HTTPBadRequest,
    HTTPInternalServerError,
    Request,
    Response,
    StreamResponse,
    json_response,
    middleware,
)
from aiohttp.web_exceptions import (
    HTTPConflict,
    HTTPCreated,
    HTTPNoContent,
    HTTPNotFound,
    HTTPOk,
)
from aiohttp.web_urldispatcher import AbstractRoute
from aiohttp_apispec import docs, request_schema, response_schema, setup_aiohttp_apispec
from aiohttp_security import check_authorized
from apolo_api_client import ApiClient as PlatformApiClient
from marshmallow import fields
from neuro_auth_client import AuthClient, Permission, check_permissions
from neuro_auth_client.security import AuthScheme, setup_security
from neuro_logging import (
    init_logging,
    setup_sentry,
)

from platform_neuro_flow_api import __version__
from platform_neuro_flow_api.identity import untrusted_user

from .config import Config, PlatformAuthConfig
from .config_factory import EnvironConfigFactory
from .postgres import make_async_engine
from .schema import (
    AttemptSchema,
    BakeImagePatchSchema,
    BakeImageSchema,
    BakeSchema,
    CacheEntrySchema,
    ClientErrorSchema,
    ConfigFileSchema,
    LiveJobSchema,
    ProjectSchema,
    TaskSchema,
    query_schema,
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
    def register(self, app: aiohttp.web.Application) -> list[AbstractRoute]:
        return app.add_routes(
            [
                aiohttp.web.get("/ping", self.handle_ping),
                aiohttp.web.get("/secured-ping", self.handle_secured_ping),
            ]
        )

    async def handle_ping(self, request: Request) -> Response:
        return Response(text="Pong")

    async def handle_secured_ping(self, request: Request) -> Response:
        await check_authorized(request)
        return Response(text="Secured Pong")


class ProjectsApiHandler(ProjectAccessMixin):
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            [
                aiohttp.web.get("", self.list_projects),
                aiohttp.web.post("", self.create_project),
                aiohttp.web.get("/by_name", self.get_project_by_name),
                aiohttp.web.get("/{id}", self.get_project),
                aiohttp.web.delete("/{id}", self.delete_project),
            ]
        )

    @property
    def storage(self) -> Storage:
        return self._app["storage"]

    @property
    def auth_client(self) -> AuthClient:
        return self._app["auth_client"]

    @docs(tags=["projects"], summary="List all users projects")
    @query_schema(
        name=fields.String(required=False),
        cluster=fields.String(required=False),
        org_name=fields.String(required=False, allow_none=True),
        owner=fields.String(required=False),
        project_name=fields.String(required=False),
    )
    @response_schema(ProjectSchema(many=True), HTTPOk.status_code)
    async def list_projects(
        self,
        request: aiohttp.web.Request,
        name: str | None = None,
        cluster: str | None = None,
        org_name: _Sentinel | str | None = sentinel,
        owner: str | None = None,
        project_name: str | None = None,
    ) -> aiohttp.web.StreamResponse:
        username = await check_authorized(request)
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

    @docs(
        tags=["projects"],
        summary="Create project",
        responses={
            HTTPCreated.status_code: {
                "description": "Project created",
                "schema": ProjectSchema(),
            },
            HTTPConflict.status_code: {
                "description": "Project with such name exists",
                "schema": ClientErrorSchema(),
            },
        },
    )
    @request_schema(ProjectSchema())
    async def create_project(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
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

    @docs(tags=["projects"], summary="Get projects by id")
    @response_schema(ProjectSchema(), HTTPOk.status_code)
    async def get_project(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        id = request.match_info["id"]
        project = await self._get_project(request, id, write=False)
        return aiohttp.web.json_response(
            data=ProjectSchema().dump(project), status=HTTPOk.status_code
        )

    @docs(tags=["projects"], summary="Get projects by id")
    @query_schema(
        name=fields.String(required=True),
        cluster=fields.String(required=True),
        owner=fields.String(required=False),
        project_name=fields.String(required=False),
        org_name=fields.String(required=False, allow_none=True),
    )
    @response_schema(ProjectSchema(), HTTPOk.status_code)
    async def get_project_by_name(
        self,
        request: aiohttp.web.Request,
        name: str,
        cluster: str,
        owner: str | None = None,
        project_name: str | None = None,
        org_name: str | None = None,
    ) -> aiohttp.web.Response:
        username = await check_authorized(request)
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

    @docs(tags=["projects"], summary="Delete project by id")
    @response_schema(ProjectSchema(), HTTPOk.status_code)
    async def delete_project(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        id = request.match_info["id"]
        await self._get_project(request, id, write=True)
        await self.storage.projects.delete(id)
        return aiohttp.web.Response(status=HTTPNoContent.status_code)


class LiveJobApiHandler(ProjectAccessMixin):
    def __init__(self, app: aiohttp.web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
            [
                aiohttp.web.get("", self.list),
                aiohttp.web.post("", self.create),
                aiohttp.web.put("/replace", self.replace),
                aiohttp.web.get("/by_yaml_id", self.get_by_yaml_id),
                aiohttp.web.get("/{id}", self.get),
            ]
        )

    @property
    def storage(self) -> Storage:
        return self._app["storage"]

    @docs(tags=["live_jobs"], summary="List live jobs in given project")
    @query_schema(project_id=fields.String(required=True))
    @response_schema(LiveJobSchema(many=True), HTTPOk.status_code)
    async def list(
        self,
        request: aiohttp.web.Request,
        project_id: str,
    ) -> aiohttp.web.StreamResponse:
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

    @docs(
        tags=["live_jobs"],
        summary="Create live job",
        responses={
            HTTPCreated.status_code: {
                "description": "Live job created",
                "schema": LiveJobSchema(),
            },
            HTTPConflict.status_code: {
                "description": "Live job with such yaml_id exists",
                "schema": ClientErrorSchema(),
            },
        },
    )
    @request_schema(LiveJobSchema())
    async def create(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
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

    @docs(
        tags=["live_jobs"],
        summary="Create live job or update by yaml_id match",
        responses={
            HTTPCreated.status_code: {
                "description": "Live job replaced",
                "schema": LiveJobSchema(),
            },
        },
    )
    @request_schema(LiveJobSchema())
    async def replace(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
        schema = LiveJobSchema()
        live_job_data = schema.load(await request.json())
        await self._get_project(request, live_job_data.project_id, write=True)
        live_job = await self.storage.live_jobs.update_or_create(live_job_data)
        return aiohttp.web.json_response(
            data=schema.dump(live_job), status=HTTPCreated.status_code
        )

    @docs(tags=["live_jobs"], summary="Get live job by id")
    @response_schema(LiveJobSchema(), HTTPOk.status_code)
    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        id = request.match_info["id"]
        try:
            live_job = await self.storage.live_jobs.get(id)
        except NotExistsError:
            raise HTTPNotFound
        await self._get_project(request, live_job.project_id, write=False)
        return aiohttp.web.json_response(
            data=LiveJobSchema().dump(live_job), status=HTTPOk.status_code
        )

    @docs(tags=["live_jobs"], summary="Get projects by id")
    @query_schema(
        project_id=fields.String(required=True),
        yaml_id=fields.String(required=True),
    )
    @response_schema(LiveJobSchema(), HTTPOk.status_code)
    async def get_by_yaml_id(
        self, request: aiohttp.web.Request, project_id: str, yaml_id: str
    ) -> aiohttp.web.Response:
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
        return self._app["storage"]

    @docs(tags=["bakes"], summary="List bakes in given project")
    @query_schema(
        project_id=fields.String(required=True),
        name=fields.String(load_default=None),
        tags=fields.List(fields.String(), load_default=()),
        since=fields.AwareDateTime(load_default=None),
        until=fields.AwareDateTime(load_default=None),
        reverse=fields.Boolean(load_default=False),
        fetch_last_attempt=fields.Boolean(load_default=False),
    )
    @response_schema(BakeSchema(many=True), HTTPOk.status_code)
    async def list(
        self,
        request: aiohttp.web.Request,
        project_id: str,
        name: str | None,
        tags: Sequence[str],
        since: datetime | None,
        until: datetime | None,
        reverse: bool,
        fetch_last_attempt: bool,
    ) -> aiohttp.web.StreamResponse:
        try:
            await self._get_project(request, project_id, write=False)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        bakes = self.storage.bakes.list(
            project_id=project_id,
            name=name,
            tags=set(tags),
            since=since,
            until=until,
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

    @docs(
        tags=["bakes"],
        summary="Create bake job",
        responses={
            HTTPCreated.status_code: {
                "description": "Bake created",
                "schema": BakeSchema(),
            },
            HTTPConflict.status_code: {
                "description": "bake with such id exists",
                "schema": ClientErrorSchema(),
            },
        },
    )
    @request_schema(BakeSchema(partial=["name", "tags"]))
    async def create(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
        schema = BakeSchema(partial=["name", "tags"])
        bake_data = schema.load(await request.json())
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

    @docs(tags=["bakes"], summary="Get bake by id")
    @query_schema(
        fetch_last_attempt=fields.Boolean(load_default=False),
    )
    @response_schema(BakeSchema(), HTTPOk.status_code)
    async def get(
        self, request: aiohttp.web.Request, fetch_last_attempt: bool
    ) -> aiohttp.web.Response:
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

    @docs(tags=["bakes"], summary="Get bake by name")
    @query_schema(
        project_id=fields.String(required=True),
        name=fields.String(required=True),
        fetch_last_attempt=fields.Boolean(load_default=False),
    )
    @response_schema(BakeSchema(), HTTPOk.status_code)
    async def get_by_name(
        self,
        request: aiohttp.web.Request,
        project_id: str,
        name: str,
        fetch_last_attempt: bool,
    ) -> aiohttp.web.Response:
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
        return self._app["storage"]

    async def _get_bake(self, bake_id: str) -> Bake:
        try:
            return await self.storage.bakes.get(bake_id)
        except NotExistsError:
            raise HTTPNotFound

    @docs(tags=["attempts"], summary="List attempts in given bake")
    @query_schema(bake_id=fields.String(required=True))
    @response_schema(AttemptSchema(many=True), HTTPOk.status_code)
    async def list(
        self,
        request: aiohttp.web.Request,
        bake_id: str,
    ) -> aiohttp.web.StreamResponse:
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

    @docs(
        tags=["attempts"],
        summary="Create bake attempt",
        responses={
            HTTPCreated.status_code: {
                "description": "Attempt created",
                "schema": AttemptSchema(),
            },
            HTTPConflict.status_code: {
                "description": "Attempt with such bake and number exists",
                "schema": ClientErrorSchema(),
            },
        },
    )
    @request_schema(AttemptSchema(partial=["executor_id"]))
    async def create(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
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

    @docs(tags=["attempts"], summary="Get attempt by id")
    @response_schema(AttemptSchema(), HTTPOk.status_code)
    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
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

    @docs(tags=["attempts"], summary="Get attempt by bake and number")
    @query_schema(
        bake_id=fields.String(required=True),
        number=fields.Integer(required=True),
    )
    @response_schema(AttemptSchema(), HTTPOk.status_code)
    async def get_by_number(
        self,
        request: aiohttp.web.Request,
        bake_id: str,
        number: int,
    ) -> aiohttp.web.Response:
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

    @docs(
        tags=["attempts"],
        summary="Update existing attempt",
        responses={
            HTTPOk.status_code: {
                "description": "Attempt replaced",
                "schema": AttemptSchema(),
            },
        },
    )
    @request_schema(AttemptSchema(partial=["executor_id"]))
    @response_schema(AttemptSchema(), HTTPOk.status_code)
    async def replace(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
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
        app.add_routes(
            [
                aiohttp.web.get("", self.list),
                aiohttp.web.post("", self.create),
                aiohttp.web.put("/replace", self.replace),
                aiohttp.web.get("/by_yaml_id", self.get_by_yaml_id),
                aiohttp.web.get("/{id}", self.get),
            ]
        )

    @property
    def storage(self) -> Storage:
        return self._app["storage"]

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

    @docs(tags=["tasks"], summary="List tasks in given attempt")
    @query_schema(attempt_id=fields.String(required=True))
    @response_schema(TaskSchema(many=True), HTTPOk.status_code)
    async def list(
        self,
        request: aiohttp.web.Request,
        attempt_id: str,
    ) -> aiohttp.web.StreamResponse:
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

    @docs(
        tags=["tasks"],
        summary="Create task",
        responses={
            HTTPCreated.status_code: {
                "description": "Bake created",
                "schema": BakeSchema(),
            },
            HTTPConflict.status_code: {
                "description": "Task already exists",
                "schema": ClientErrorSchema(),
            },
        },
    )
    @request_schema(TaskSchema())
    async def create(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
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

    @docs(tags=["tasks"], summary="Get task by id")
    @response_schema(TaskSchema(), HTTPOk.status_code)
    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
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

    @docs(tags=["tasks"], summary="Get tasks by id")
    @query_schema(
        attempt_id=fields.String(required=True),
        yaml_id=fields.String(required=True),
    )
    async def get_by_yaml_id(
        self, request: aiohttp.web.Request, attempt_id: str, yaml_id: str
    ) -> aiohttp.web.Response:
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

    @docs(
        tags=["tasks"],
        summary="Update task",
        responses={
            HTTPOk.status_code: {
                "description": "Task data replaced",
                "schema": TaskSchema(),
            },
        },
    )
    @request_schema(TaskSchema())
    @response_schema(AttemptSchema(), HTTPOk.status_code)
    async def replace(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
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
        app.add_routes(
            [
                aiohttp.web.post("", self.create),
                aiohttp.web.get("/{id}", self.get),
            ]
        )

    @property
    def storage(self) -> Storage:
        return self._app["storage"]

    async def _get_bake(self, bake_id: str) -> Bake:
        try:
            return await self.storage.bakes.get(bake_id)
        except NotExistsError:
            raise HTTPNotFound

    @docs(
        tags=["config_files"],
        summary="Create config file",
        responses={
            HTTPCreated.status_code: {
                "description": "Bake created",
                "schema": ConfigFileSchema(),
            },
            HTTPConflict.status_code: {
                "description": "Config file already exists",
                "schema": ClientErrorSchema(),
            },
        },
    )
    @request_schema(ConfigFileSchema())
    async def create(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
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

    @docs(tags=["config_files"], summary="Get config file by id")
    @response_schema(ConfigFileSchema(), HTTPOk.status_code)
    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
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
        app.add_routes(
            [
                aiohttp.web.post("", self.create),
                aiohttp.web.delete("", self.delete),
                aiohttp.web.get("/by_key", self.get_by_key),
                aiohttp.web.get("/{id}", self.get),
            ]
        )

    @property
    def storage(self) -> Storage:
        return self._app["storage"]

    @docs(
        tags=["cache_entries"],
        summary="Create cache entry",
        responses={
            HTTPCreated.status_code: {
                "description": "Cache entry created",
                "schema": CacheEntrySchema(),
            },
        },
    )
    @request_schema(CacheEntrySchema(partial=["raw_id"]))
    async def create(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
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

    @docs(tags=["cache_entries"], summary="Get cache entry by id")
    @response_schema(CacheEntrySchema(), HTTPOk.status_code)
    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        id = request.match_info["id"]
        try:
            cache_entry = await self.storage.cache_entries.get(id)
        except NotExistsError:
            raise HTTPNotFound
        await self._get_project(request, cache_entry.project_id, write=False)
        return aiohttp.web.json_response(
            data=CacheEntrySchema().dump(cache_entry), status=HTTPOk.status_code
        )

    @docs(tags=["cache_entries"], summary="Get cache entry by key")
    @query_schema(
        project_id=fields.String(required=True),
        task_id=fields.String(required=True),
        batch=fields.String(required=True),
        key=fields.String(required=True),
    )
    @response_schema(CacheEntrySchema(), HTTPOk.status_code)
    async def get_by_key(
        self,
        request: aiohttp.web.Request,
        project_id: str,
        task_id: str,
        batch: str,
        key: str,
    ) -> aiohttp.web.Response:
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

    @docs(tags=["cache_entries"], summary="Clear cache entries")
    @query_schema(
        project_id=fields.String(required=True),
        task_id=fields.String(required=False),
        batch=fields.String(required=False),
    )
    async def delete(
        self,
        request: aiohttp.web.Request,
        project_id: str,
        task_id: str | None = None,
        batch: str | None = None,
    ) -> aiohttp.web.StreamResponse:
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
        app.add_routes(
            [
                aiohttp.web.get("", self.list),
                aiohttp.web.post("", self.create),
                aiohttp.web.get("/by_ref", self.get_by_ref),
                aiohttp.web.patch("/{id}", self.patch),
                aiohttp.web.get("/{id}", self.get),
            ]
        )

    @property
    def storage(self) -> Storage:
        return self._app["storage"]

    async def _get_bake(self, bake_id: str) -> Bake:
        try:
            return await self.storage.bakes.get(bake_id)
        except NotExistsError:
            raise HTTPNotFound

    @docs(tags=["bake_images"], summary="List bakes images in given bake")
    @query_schema(bake_id=fields.String(required=True))
    @response_schema(BakeImageSchema(many=True), HTTPOk.status_code)
    async def list(
        self,
        request: aiohttp.web.Request,
        bake_id: str,
    ) -> aiohttp.web.StreamResponse:
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

    @docs(
        tags=["bake_images"],
        summary="Create bake image",
        responses={
            HTTPCreated.status_code: {
                "description": "Bake image created",
                "schema": BakeImageSchema(),
            },
            HTTPConflict.status_code: {
                "description": "Bake image with such bake and ref exists",
                "schema": ClientErrorSchema(),
            },
        },
    )
    @request_schema(
        BakeImageSchema(
            partial=["context_on_storage", "dockerfile_rel", "builder_job_id"]
        )
    )
    async def create(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
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

    @docs(tags=["bake_images"], summary="Get attempt by id")
    @response_schema(BakeImageSchema(), HTTPOk.status_code)
    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
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

    @docs(tags=["bake_images"], summary="Get bake image by bake and ref")
    @query_schema(
        bake_id=fields.String(required=True),
        ref=fields.String(required=True),
    )
    @response_schema(BakeImageSchema(), HTTPOk.status_code)
    async def get_by_ref(
        self,
        request: aiohttp.web.Request,
        bake_id: str,
        ref: str,
    ) -> aiohttp.web.Response:
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

    @docs(
        tags=["bake_images"],
        summary="Update existing bake image",
        responses={
            HTTPOk.status_code: {
                "description": "Bake image patched",
                "schema": BakeImageSchema(),
            },
        },
    )
    @request_schema(BakeImagePatchSchema())
    @response_schema(BakeImageSchema(), HTTPOk.status_code)
    async def patch(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
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
    except ValueError as e:
        payload = {"error": str(e)}
        return json_response(payload, status=HTTPBadRequest.status_code)
    except aiohttp.web.HTTPException:
        raise
    except Exception as e:
        msg_str = f"Unexpected exception: {str(e)}. Path with query: {request.path_qs}."
        logging.exception(msg_str)
        payload = {"error": msg_str}
        return json_response(payload, status=HTTPInternalServerError.status_code)


async def create_projects_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = ProjectsApiHandler(app, config)
    handler.register(app)
    return app


async def create_live_jobs_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = LiveJobApiHandler(app, config)
    handler.register(app)
    return app


async def create_bakes_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = BakeApiHandler(app, config)
    handler.register(app)
    return app


async def create_attempts_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = AttemptApiHandler(app, config)
    handler.register(app)
    return app


async def create_tasks_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = TaskApiHandler(app, config)
    handler.register(app)
    return app


async def create_config_files_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = ConfigFileApiHandler(app, config)
    handler.register(app)
    return app


async def create_cache_entries_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = CacheEntryApiHandler(app, config)
    handler.register(app)
    return app


async def create_bake_images_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = BakeImagesApiHandler(app, config)
    handler.register(app)
    return app


@asynccontextmanager
async def create_auth_client(config: PlatformAuthConfig) -> AsyncIterator[AuthClient]:
    async with AuthClient(config.url, config.token) as client:
        yield client


async def add_version_to_header(request: Request, response: StreamResponse) -> None:
    response.headers["X-Service-Version"] = f"platform-neuro-flow-api/{__version__}"


async def create_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

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

            app["projects_app"]["storage"] = storage
            app["projects_app"]["auth_client"] = auth_client
            app["live_jobs_app"]["storage"] = storage
            app["bakes_app"]["storage"] = storage
            app["attempts_app"]["storage"] = storage
            app["tasks_app"]["storage"] = storage
            app["cache_entries_app"]["storage"] = storage
            app["config_files_app"]["storage"] = storage
            app["bake_images_app"]["storage"] = storage

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)
    app["api_v1_app"] = api_v1_app

    projects_app = await create_projects_app(config)
    app["projects_app"] = projects_app
    api_v1_app.add_subapp("/flow/projects", projects_app)

    live_jobs_app = await create_live_jobs_app(config)
    app["live_jobs_app"] = live_jobs_app
    api_v1_app.add_subapp("/flow/live_jobs", live_jobs_app)

    bakes_app = await create_bakes_app(config)
    app["bakes_app"] = bakes_app
    api_v1_app.add_subapp("/flow/bakes", bakes_app)

    attempts_app = await create_attempts_app(config)
    app["attempts_app"] = attempts_app
    api_v1_app.add_subapp("/flow/attempts", attempts_app)

    tasks_app = await create_tasks_app(config)
    app["tasks_app"] = tasks_app
    api_v1_app.add_subapp("/flow/tasks", tasks_app)

    cache_entries_app = await create_cache_entries_app(config)
    app["cache_entries_app"] = cache_entries_app
    api_v1_app.add_subapp("/flow/cache_entries", cache_entries_app)

    config_files_app = await create_config_files_app(config)
    app["config_files_app"] = config_files_app
    api_v1_app.add_subapp("/flow/config_files", config_files_app)

    bake_images_app = await create_bake_images_app(config)
    app["bake_images_app"] = bake_images_app
    api_v1_app.add_subapp("/flow/bake_images", bake_images_app)

    app.add_subapp("/api/v1", api_v1_app)

    if config.enable_docs:
        prefix = "/api/docs/v1/flow"
        setup_aiohttp_apispec(
            app=app,
            title="Neuro Flow API documentation",
            version="v1",
            url=f"{prefix}/swagger.json",
            static_path=f"{prefix}/static",
            swagger_path=f"{prefix}/ui",
            security=[{"jwt": []}],
            securityDefinitions={
                "jwt": {"type": "apiKey", "name": "Authorization", "in": "header"},
            },
        )

    app.on_response_prepare.append(add_version_to_header)

    return app


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    setup_sentry()
    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )
