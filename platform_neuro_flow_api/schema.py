import functools
from typing import Any, Callable, Mapping, TypeVar

import aiohttp.web
from aiohttp_apispec import querystring_schema
from marshmallow import Schema, fields, post_load

from platform_neuro_flow_api.storage.base import LiveJobData, ProjectData


F = TypeVar("F", bound=Callable[..., Any])


def query_schema(**kwargs: fields.Field) -> Callable[[F], F]:
    schema: Schema = Schema.from_dict(kwargs)()  # type: ignore

    def _decorator(handler: F) -> F:
        @querystring_schema(schema)
        @functools.wraps(handler)
        async def _wrapped(self: Any, request: aiohttp.web.Request) -> Any:
            validated = schema.load(request.query)
            return await handler(self, request, **validated)

        return _wrapped

    return _decorator


class ProjectSchema(Schema):
    id = fields.String(required=True, dump_only=True)
    name = fields.String(required=True)
    owner = fields.String(required=True, dump_only=True)
    cluster = fields.String(required=True)

    @post_load
    def make_project_data(self, data: Mapping[str, Any], **kwargs: Any) -> ProjectData:
        # Parse object to dataclass here
        return ProjectData(
            name=data["name"],
            owner=self.context["username"],
            cluster=data["cluster"],
        )


class LiveJobSchema(Schema):
    id = fields.String(required=True, dump_only=True)
    yaml_id = fields.String(required=True)
    project_id = fields.String(required=True)
    multi = fields.Boolean(required=True)
    tags = fields.List(fields.String(), required=True)

    @post_load
    def make_live_job_data(self, data: Mapping[str, Any], **kwargs: Any) -> LiveJobData:
        # Parse object to dataclass here
        return LiveJobData(**data)


class ClientErrorSchema(Schema):
    code = fields.String(required=True)
    description = fields.String(required=True)
