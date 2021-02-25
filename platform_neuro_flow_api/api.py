import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable, Optional

import aiohttp
import aiohttp.web
import aiohttp_cors
import pkg_resources
import sentry_sdk
from aiohttp.web import (
    HTTPBadRequest,
    HTTPInternalServerError,
    Request,
    Response,
    StreamResponse,
    json_response,
    middleware,
)
from aiohttp.web_exceptions import HTTPConflict, HTTPCreated, HTTPNotFound, HTTPOk
from aiohttp_apispec import docs, request_schema, response_schema, setup_aiohttp_apispec
from aiohttp_security import check_authorized
from marshmallow import fields
from neuro_auth_client import AuthClient
from neuro_auth_client.security import AuthScheme, setup_security
from platform_logging import init_logging
from sentry_sdk import set_tag
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from .config import Config, CORSConfig, PlatformAuthConfig
from .config_factory import EnvironConfigFactory
from .postgres import create_postgres_pool
from .schema import ClientErrorSchema, ProjectSchema, query_schema
from .storage.base import ExistsError, NotExistsError, Storage
from .storage.postgres import PostgresStorage
from .utils import ndjson_error_handler


logger = logging.getLogger(__name__)


class ApiHandler:
    def register(self, app: aiohttp.web.Application) -> None:
        app.add_routes(
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

    def _accepts_ndjson(self, request: aiohttp.web.Request) -> bool:
        accept = request.headers.get("Accept", "")
        return "application/x-ndjson" in accept

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
        if self._accepts_ndjson(request):
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
        id_or_name = request.match_info["id"]
        try:
            project = await self.storage.projects.get(id_or_name)
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


async def create_api_v1_app() -> aiohttp.web.Application:
    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)
    return api_v1_app


async def create_projects_app(config: Config) -> aiohttp.web.Application:
    app = aiohttp.web.Application()
    handler = ProjectsApiHandler(app, config)
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

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = await create_api_v1_app()
    app["api_v1_app"] = api_v1_app

    projects_app = await create_projects_app(config)
    app["projects_app"] = projects_app
    api_v1_app.add_subapp("/flow/projects", projects_app)

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

    return app


def main() -> None:  # pragma: no coverage
    init_logging()
    config = EnvironConfigFactory().create()
    logging.info("Loaded config: %r", config)

    if config.sentry:
        sentry_sdk.init(dsn=config.sentry.url, integrations=[AioHttpIntegration()])

        set_tag("cluster", config.sentry.cluster)
        set_tag("app", "platformneuroflowapi")

    aiohttp.web.run_app(
        create_app(config), host=config.server.host, port=config.server.port
    )
