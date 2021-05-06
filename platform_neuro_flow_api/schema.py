import functools
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, TypeVar

import aiohttp.web
from aiohttp_apispec import querystring_schema
from marshmallow import Schema, fields, post_load

from platform_neuro_flow_api.storage.base import (
    AttemptData,
    BakeData,
    CacheEntryData,
    ConfigFileData,
    FullID,
    LiveJobData,
    ProjectData,
    TaskData,
    TaskStatus,
    TaskStatusItem,
)


F = TypeVar("F", bound=Callable[..., Any])


def query_schema(**kwargs: fields.Field) -> Callable[[F], F]:
    schema: Schema = Schema.from_dict(kwargs)()  # type: ignore

    def _decorator(handler: F) -> F:
        @querystring_schema(schema)
        @functools.wraps(handler)
        async def _wrapped(self: Any, request: aiohttp.web.Request) -> Any:
            query_data = {
                key: request.query.getall(key)
                if len(request.query.getall(key)) > 1
                or isinstance(schema.fields.get(key), fields.List)
                else request.query[key]
                for key in request.query.keys()
            }
            validated = schema.load(query_data)
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


class FullIDField(fields.String):
    def _deserialize(self, *args: Any, **kwargs: Any) -> FullID:
        res: str = super()._deserialize(*args, **kwargs)
        return tuple(res.split("."))

    def _serialize(
        self, value: Optional[FullID], *args: Any, **kwargs: Any
    ) -> Optional[str]:
        if value is None:
            return None
        return super()._serialize(".".join(value), *args, **kwargs)


class BakeSchema(Schema):
    id = fields.String(required=True, dump_only=True)
    project_id = fields.String(required=True)
    batch = fields.String(required=True)
    created_at = fields.AwareDateTime(
        missing=lambda: datetime.now(timezone.utc)
    )  # when
    graphs = fields.Dict(
        keys=FullIDField(),
        values=fields.Dict(keys=FullIDField(), values=fields.List(FullIDField())),
        required=True,
    )
    params = fields.Dict(keys=fields.String(), values=fields.String())
    tags = fields.List(fields.String(), required=True, metadata=dict(doc_default=()))

    @post_load
    def make_bake_data(self, data: Dict[str, Any], **kwargs: Any) -> BakeData:
        return BakeData(**data)


class ConfigFileSchema(Schema):
    id = fields.String(required=True, dump_only=True)
    bake_id = fields.String(required=True)
    filename = fields.String(required=True)
    content = fields.String(required=True)

    @post_load
    def make_config_file_data(
        self, data: Mapping[str, Any], **kwargs: Any
    ) -> ConfigFileData:
        # Parse object to dataclass here
        return ConfigFileData(**data)


class ConfigsMetaSchema(Schema):
    id = fields.String(required=True, dump_only=True)
    workspace = fields.String(required=True)
    flow_config_id = fields.String(required=True)
    project_config_id = fields.String(allow_none=True)
    action_config_ids = fields.Dict(
        keys=fields.String(required=True), values=fields.String(required=True)
    )


class TaskStatusField(fields.String):
    def _deserialize(self, *args: Any, **kwargs: Any) -> TaskStatus:
        res: str = super()._deserialize(*args, **kwargs)
        return TaskStatus(res)

    def _serialize(
        self, value: Optional[TaskStatus], *args: Any, **kwargs: Any
    ) -> Optional[str]:
        if value is None:
            return None
        return super()._serialize(value.value, *args, **kwargs)


class AttemptSchema(Schema):
    id = fields.String(required=True, dump_only=True)
    bake_id = fields.String(required=True)
    number = fields.Integer(required=True, strict=True)
    created_at = fields.AwareDateTime(
        missing=lambda: datetime.now(timezone.utc)
    )  # when
    result = TaskStatusField(required=True)
    configs_meta = fields.Nested(ConfigsMetaSchema(), required=True)

    @post_load
    def make_attempt(self, data: Dict[str, Any], **kwargs: Any) -> AttemptData:
        return AttemptData(**data)


class TaskStatusItemSchema(Schema):
    created_at = fields.AwareDateTime(
        required=True, attribute="when", data_key="created_at"
    )
    status = TaskStatusField(required=True)


class TaskSchema(Schema):
    id = fields.String(required=True, dump_only=True)
    yaml_id = FullIDField(required=True)
    attempt_id = fields.String(required=True)
    raw_id = fields.String(required=True)  # empty string for no id
    outputs = fields.Dict(
        keys=fields.String(required=True), values=fields.String(required=True)
    )
    state = fields.Dict(
        keys=fields.String(required=True), values=fields.String(required=True)
    )
    statuses = fields.List(fields.Nested(TaskStatusItemSchema()), required=True)

    @post_load
    def make_task_data(self, data: Mapping[str, Any], **kwargs: Any) -> TaskData:
        # Parse object to dataclass here
        kwargs = dict(data)
        statuses = kwargs.pop("statuses")
        return TaskData(
            statuses=[
                TaskStatusItem(when=i["when"], status=i["status"]) for i in statuses
            ],
            **kwargs
        )


class CacheEntrySchema(Schema):
    id = fields.String(required=True, dump_only=True)
    project_id = fields.String(required=True)
    task_id = FullIDField(required=True)
    batch = fields.String(required=True)
    key = fields.String(required=True)
    created_at = fields.AwareDateTime(missing=lambda: datetime.now(timezone.utc))
    outputs = fields.Dict(values=fields.String(), required=True)
    state = fields.Dict(values=fields.String(), required=True)

    @post_load
    def make_cache_entry_data(
        self, data: Dict[str, Any], **kwargs: Any
    ) -> CacheEntryData:
        return CacheEntryData(**data)


class ClientErrorSchema(Schema):
    code = fields.String(required=True)
    description = fields.String(required=True)
