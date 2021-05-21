import logging
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from typing import AsyncIterator, Awaitable, Callable, List, Optional, Sequence

import aiohttp
import aiohttp.web
import aiohttp_cors
import pkg_resources
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
from marshmallow import fields
from neuro_auth_client import AuthClient
from neuro_auth_client.security import AuthScheme, setup_security
from platform_logging import (
    init_logging,
    notrace,
    setup_sentry,
    setup_zipkin,
    setup_zipkin_tracer,
)

from .config import Config, CORSConfig, PlatformAuthConfig
from .config_factory import EnvironConfigFactory
from .postgres import create_postgres_pool
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
from .storage.base import Attempt, Bake, ExistsError, NotExistsError, Storage
from .storage.postgres import PostgresStorage
from .utils import auto_close, ndjson_error_handler


logger = logging.getLogger(__name__)


def accepts_ndjson(request: aiohttp.web.Request) -> bool:
    accept = request.headers.get("Accept", "")
    return "application/x-ndjson" in accept


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> List[AbstractRoute]:
        return app.add_routes(
            [
                aiohttp.web.get("/ping", self.handle_ping),
                aiohttp.web.get("/secured-ping", self.handle_secured_ping),
            ]
        )

    @notrace
    async def handle_ping(self, request: Request) -> Response:
        return Response(text="Pong")

    @notrace
    async def handle_secured_ping(self, request: Request) -> Response:
        await check_authorized(request)
        return Response(text="Secured Pong")


class ProjectsApiHandler:
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
            ]
        )

    @property
    def storage(self) -> Storage:
        return self._app["storage"]

    @docs(tags=["projects"], summary="List all users projects")
    @query_schema(
        name=fields.String(required=False), cluster=fields.String(required=False)
    )
    @response_schema(ProjectSchema(many=True), HTTPOk.status_code)
    async def list_projects(
        self,
        request: aiohttp.web.Request,
        name: Optional[str] = None,
        cluster: Optional[str] = None,
    ) -> aiohttp.web.StreamResponse:
        username = await check_authorized(request)
        projects = self.storage.projects.list(
            owner=username, name=name, cluster=cluster
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
            else:
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
        username = await check_authorized(request)
        schema = ProjectSchema()
        schema.context["username"] = username
        project_data = schema.load(await request.json())
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
        username = await check_authorized(request)
        id = request.match_info["id"]
        try:
            project = await self.storage.projects.get(id)
        except NotExistsError:
            raise HTTPNotFound
        if project.owner != username:
            raise HTTPNotFound
        return aiohttp.web.json_response(
            data=ProjectSchema().dump(project), status=HTTPOk.status_code
        )

    @docs(tags=["projects"], summary="Get projects by id")
    @query_schema(
        name=fields.String(required=True), cluster=fields.String(required=True)
    )
    @response_schema(ProjectSchema(), HTTPOk.status_code)
    async def get_project_by_name(
        self, request: aiohttp.web.Request, name: str, cluster: str
    ) -> aiohttp.web.Response:
        username = await check_authorized(request)
        try:
            project = await self.storage.projects.get_by_name(name, username, cluster)
        except NotExistsError:
            raise HTTPNotFound
        return aiohttp.web.json_response(
            data=ProjectSchema().dump(project), status=HTTPOk.status_code
        )


class LiveJobApiHandler:
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

    async def _check_project(self, username: str, project_id: str) -> None:
        try:
            project = await self.storage.projects.get(project_id)
        except NotExistsError:
            raise HTTPNotFound
        if project.owner != username:
            raise HTTPNotFound

    @docs(tags=["live_jobs"], summary="List live jobs in given project")
    @query_schema(project_id=fields.String(required=True))
    @response_schema(LiveJobSchema(many=True), HTTPOk.status_code)
    async def list(
        self,
        request: aiohttp.web.Request,
        project_id: str,
    ) -> aiohttp.web.StreamResponse:
        username = await check_authorized(request)
        try:
            await self._check_project(username, project_id)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        live_jobs = self.storage.live_jobs.list(project_id=project_id)
        async with auto_close(live_jobs):
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for live_job in live_jobs:
                        payload_line = LiveJobSchema().dumps(live_job)
                        await response.write(payload_line.encode() + b"\n")
                return response
            else:
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
        username = await check_authorized(request)
        schema = LiveJobSchema()
        live_job_data = schema.load(await request.json())
        await self._check_project(username, live_job_data.project_id)
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
        username = await check_authorized(request)
        schema = LiveJobSchema()
        live_job_data = schema.load(await request.json())
        await self._check_project(username, live_job_data.project_id)
        live_job = await self.storage.live_jobs.update_or_create(live_job_data)
        return aiohttp.web.json_response(
            data=schema.dump(live_job), status=HTTPCreated.status_code
        )

    @docs(tags=["live_jobs"], summary="Get live job by id")
    @response_schema(LiveJobSchema(), HTTPOk.status_code)
    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        username = await check_authorized(request)
        id = request.match_info["id"]
        try:
            live_job = await self.storage.live_jobs.get(id)
        except NotExistsError:
            raise HTTPNotFound
        await self._check_project(username, live_job.project_id)
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
        username = await check_authorized(request)
        await self._check_project(username, project_id)
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


class BakeApiHandler:
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

    async def _check_project(self, username: str, project_id: str) -> None:
        try:
            project = await self.storage.projects.get(project_id)
        except NotExistsError:
            raise HTTPNotFound
        if project.owner != username:
            raise HTTPNotFound

    @docs(tags=["bakes"], summary="List bakes in given project")
    @query_schema(
        project_id=fields.String(required=True),
        name=fields.String(missing=None),
        tags=fields.List(fields.String(), missing=tuple()),
    )
    @response_schema(BakeSchema(many=True), HTTPOk.status_code)
    async def list(
        self,
        request: aiohttp.web.Request,
        project_id: str,
        name: Optional[str],
        tags: Sequence[str],
    ) -> aiohttp.web.StreamResponse:
        username = await check_authorized(request)
        try:
            await self._check_project(username, project_id)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        bakes = self.storage.bakes.list(
            project_id=project_id, name=name, tags=set(tags)
        )
        async with auto_close(bakes):
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for bake in bakes:
                        payload_line = BakeSchema().dumps(bake)
                        await response.write(payload_line.encode() + b"\n")
                return response
            else:
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
        username = await check_authorized(request)
        schema = BakeSchema(partial=["name", "tags"])
        bake_data = schema.load(await request.json())
        await self._check_project(username, bake_data.project_id)
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
    @response_schema(BakeSchema(), HTTPOk.status_code)
    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        username = await check_authorized(request)
        id = request.match_info["id"]
        try:
            bake = await self.storage.bakes.get(id)
        except NotExistsError:
            raise HTTPNotFound
        await self._check_project(username, bake.project_id)
        return aiohttp.web.json_response(
            data=BakeSchema().dump(bake), status=HTTPOk.status_code
        )

    @docs(tags=["bakes"], summary="Get bake by name")
    @query_schema(
        project_id=fields.String(required=True),
        name=fields.String(required=True),
    )
    @response_schema(BakeSchema(), HTTPOk.status_code)
    async def get_by_name(
        self,
        request: aiohttp.web.Request,
        project_id: str,
        name: str,
    ) -> aiohttp.web.Response:
        username = await check_authorized(request)
        try:
            bake = await self.storage.bakes.get_by_name(
                project_id=project_id, name=name
            )
        except NotExistsError:
            raise HTTPNotFound
        await self._check_project(username, bake.project_id)
        return aiohttp.web.json_response(
            data=BakeSchema().dump(bake), status=HTTPOk.status_code
        )


class AttemptApiHandler:
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

    async def _check_project(self, username: str, project_id: str) -> None:
        try:
            project = await self.storage.projects.get(project_id)
        except NotExistsError:
            raise HTTPNotFound
        if project.owner != username:
            raise HTTPNotFound

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
        username = await check_authorized(request)
        try:
            bake = await self._get_bake(bake_id)
            await self._check_project(username, bake.project_id)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        attempts = self.storage.attempts.list(bake_id=bake_id)
        async with auto_close(attempts):
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for attempt in attempts:
                        payload_line = AttemptSchema().dumps(attempt)
                        await response.write(payload_line.encode() + b"\n")
                return response
            else:
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
        username = await check_authorized(request)
        schema = AttemptSchema(partial=["executor_id"])
        attempt_data = schema.load(await request.json())
        bake = await self._get_bake(attempt_data.bake_id)
        await self._check_project(username, bake.project_id)
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
        username = await check_authorized(request)
        id = request.match_info["id"]
        try:
            attempt = await self.storage.attempts.get(id)
        except NotExistsError:
            raise HTTPNotFound
        bake = await self._get_bake(attempt.bake_id)
        await self._check_project(username, bake.project_id)
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
        username = await check_authorized(request)
        bake = await self._get_bake(bake_id)
        await self._check_project(username, bake.project_id)
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
        username = await check_authorized(request)
        schema = AttemptSchema(partial=["executor_id"])
        attempt_data = schema.load(await request.json())
        attempt = await self.storage.attempts.get_by_number(
            attempt_data.bake_id, attempt_data.number
        )
        bake = await self._get_bake(attempt_data.bake_id)
        await self._check_project(username, bake.project_id)
        new_attempt = replace(
            attempt, result=attempt_data.result, executor_id=attempt_data.executor_id
        )
        await self.storage.attempts.update(new_attempt)
        return aiohttp.web.json_response(
            data=schema.dump(attempt_data), status=HTTPOk.status_code
        )


class TaskApiHandler:
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

    async def _check_project(self, username: str, project_id: str) -> None:
        try:
            project = await self.storage.projects.get(project_id)
        except NotExistsError:
            raise HTTPNotFound
        if project.owner != username:
            raise HTTPNotFound

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
        username = await check_authorized(request)
        try:
            attempt = await self._get_attempt(attempt_id)
            bake = await self._get_bake(attempt.bake_id)
            await self._check_project(username, bake.project_id)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        tasks = self.storage.tasks.list(attempt_id=attempt_id)
        async with auto_close(tasks):
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for task in tasks:
                        payload_line = TaskSchema().dumps(task)
                        await response.write(payload_line.encode() + b"\n")
                return response
            else:
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
        username = await check_authorized(request)
        schema = TaskSchema()
        task_data = schema.load(await request.json())
        attempt = await self._get_attempt(task_data.attempt_id)
        bake = await self._get_bake(attempt.bake_id)
        await self._check_project(username, bake.project_id)
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
        username = await check_authorized(request)
        id = request.match_info["id"]
        try:
            task = await self.storage.tasks.get(id)
        except NotExistsError:
            raise HTTPNotFound
        attempt = await self._get_attempt(task.attempt_id)
        bake = await self._get_bake(attempt.bake_id)
        await self._check_project(username, bake.project_id)
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
        username = await check_authorized(request)
        attempt = await self._get_attempt(attempt_id)
        bake = await self._get_bake(attempt.bake_id)
        await self._check_project(username, bake.project_id)
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
        username = await check_authorized(request)
        schema = TaskSchema()
        task_data = schema.load(await request.json())
        task = await self.storage.tasks.get_by_yaml_id(
            task_data.yaml_id, task_data.attempt_id
        )
        attempt = await self._get_attempt(task_data.attempt_id)
        bake = await self._get_bake(attempt.bake_id)
        await self._check_project(username, bake.project_id)

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


class ConfigFileApiHandler:
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

    async def _check_project(self, username: str, project_id: str) -> None:
        try:
            project = await self.storage.projects.get(project_id)
        except NotExistsError:
            raise HTTPNotFound
        if project.owner != username:
            raise HTTPNotFound

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
        username = await check_authorized(request)
        schema = ConfigFileSchema()
        config_file_data = schema.load(await request.json())
        bake = await self._get_bake(config_file_data.bake_id)
        await self._check_project(username, bake.project_id)
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
        username = await check_authorized(request)
        id = request.match_info["id"]
        try:
            config_file = await self.storage.config_files.get(id)
        except NotExistsError:
            raise HTTPNotFound
        bake = await self._get_bake(config_file.bake_id)
        await self._check_project(username, bake.project_id)
        return aiohttp.web.json_response(
            data=ConfigFileSchema().dump(config_file), status=HTTPOk.status_code
        )


class CacheEntryApiHandler:
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

    async def _check_project(self, username: str, project_id: str) -> None:
        try:
            project = await self.storage.projects.get(project_id)
        except NotExistsError:
            raise HTTPNotFound
        if project.owner != username:
            raise HTTPNotFound

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
    @request_schema(CacheEntrySchema())
    async def create(
        self,
        request: aiohttp.web.Request,
    ) -> aiohttp.web.Response:
        username = await check_authorized(request)
        schema = CacheEntrySchema()
        data = schema.load(await request.json())
        await self._check_project(username, data.project_id)
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
            data=schema.dump(cache_entry), status=HTTPCreated.status_code
        )

    @docs(tags=["cache_entries"], summary="Get cache entry by id")
    @response_schema(CacheEntrySchema(), HTTPOk.status_code)
    async def get(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        username = await check_authorized(request)
        id = request.match_info["id"]
        try:
            cache_entry = await self.storage.cache_entries.get(id)
        except NotExistsError:
            raise HTTPNotFound
        await self._check_project(username, cache_entry.project_id)
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
        username = await check_authorized(request)
        await self._check_project(username, project_id)
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
        task_id: Optional[str] = None,
        batch: Optional[str] = None,
    ) -> aiohttp.web.StreamResponse:
        username = await check_authorized(request)
        await self._check_project(username, project_id)
        await self.storage.cache_entries.delete_all(
            project_id=project_id,
            batch=batch,
            task_id=tuple(task_id.split(".")) if task_id else None,
        )
        return aiohttp.web.Response(status=HTTPNoContent.status_code)


class BakeImagesApiHandler:
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

    async def _check_project(self, username: str, project_id: str) -> None:
        try:
            project = await self.storage.projects.get(project_id)
        except NotExistsError:
            raise HTTPNotFound
        if project.owner != username:
            raise HTTPNotFound

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
        username = await check_authorized(request)
        try:
            bake = await self._get_bake(bake_id)
            await self._check_project(username, bake.project_id)
        except HTTPNotFound:
            return aiohttp.web.json_response(data=[], status=HTTPOk.status_code)
        bake_images = self.storage.bake_images.list(bake_id=bake_id)
        async with auto_close(bake_images):
            if accepts_ndjson(request):
                response = aiohttp.web.StreamResponse()
                response.headers["Content-Type"] = "application/x-ndjson"
                await response.prepare(request)
                async with ndjson_error_handler(request, response):
                    async for image in bake_images:
                        payload_line = BakeImageSchema().dumps(image)
                        await response.write(payload_line.encode() + b"\n")
                return response
            else:
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
        username = await check_authorized(request)
        schema = BakeImageSchema(
            partial=["context_on_storage", "dockerfile_rel", "builder_job_id"]
        )
        image_data = schema.load(await request.json())
        bake = await self._get_bake(image_data.bake_id)
        await self._check_project(username, bake.project_id)
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
        username = await check_authorized(request)
        id = request.match_info["id"]
        try:
            image = await self.storage.bake_images.get(id)
        except NotExistsError:
            raise HTTPNotFound
        bake = await self._get_bake(image.bake_id)
        await self._check_project(username, bake.project_id)
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
        username = await check_authorized(request)
        bake = await self._get_bake(bake_id)
        await self._check_project(username, bake.project_id)
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
        username = await check_authorized(request)
        id = request.match_info["id"]
        try:
            image = await self.storage.bake_images.get(id)
        except NotExistsError:
            raise HTTPNotFound
        bake = await self._get_bake(image.bake_id)
        await self._check_project(username, bake.project_id)
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


def _setup_cors(app: aiohttp.web.Application, config: CORSConfig) -> None:
    if not config.allowed_origins:
        return

    logger.info(f"Setting up CORS with allowed origins: {config.allowed_origins}")
    default_options = aiohttp_cors.ResourceOptions(
        allow_credentials=True,
        expose_headers="*",
        allow_headers="*",
    )
    cors = aiohttp_cors.setup(
        app, defaults={origin: default_options for origin in config.allowed_origins}
    )
    for route in app.router.routes():
        logger.debug(f"Setting up CORS for {route}")
        cors.add(route)


package_version = pkg_resources.get_distribution("platform-neuro-flow-api").version


async def add_version_to_header(request: Request, response: StreamResponse) -> None:
    response.headers["X-Service-Version"] = f"platform-neuro-flow-api/{package_version}"


async def create_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app["config"] = config

    async def _init_app(app: aiohttp.web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing Auth client")
            auth_client = await exit_stack.enter_async_context(
                create_auth_client(config.platform_auth)
            )

            logger.info("Initializing Postgres connection pool")
            postgres_pool = await exit_stack.enter_async_context(
                create_postgres_pool(config.postgres)
            )

            logger.info("Initializing PostgresStorage")
            storage: Storage = PostgresStorage(postgres_pool)

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

            logger.info("Initializing Service")
            app["projects_app"]["storage"] = storage
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
    probes_routes = api_v1_handler.register(api_v1_app)
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

    _setup_cors(app, config.cors)
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

    if config.zipkin:
        setup_zipkin(app, skip_routes=probes_routes)

    return app


def setup_tracing(config: Config) -> None:
    if config.zipkin:
        setup_zipkin_tracer(
            config.zipkin.app_name,
            config.server.host,
            config.server.port,
            config.zipkin.url,
            config.zipkin.sample_rate,
        )

    if config.sentry:
        setup_sentry(
            config.sentry.dsn,
            app_name=config.sentry.app_name,
            cluster_name=config.sentry.cluster_name,
            sample_rate=config.sentry.sample_rate,
        )


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)
    setup_tracing(config)
    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )
