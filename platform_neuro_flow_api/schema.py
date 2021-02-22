from typing import Any

from marshmallow import Schema, fields, post_load, validate


class SampleSchema(Schema):
    int = fields.Integer(required=True, validate=validate.Range(min=0))
    string = fields.String(required=False, allow_none=True)
    date = fields.DateTime(required=True)

    @post_load
    def make_sample(self, data: Any, **kwargs: Any) -> Any:
        # Parse object to dataclass here
        return data


class ClientErrorSchema(Schema):
    code = fields.String(required=True)
    description = fields.String(required=True)
