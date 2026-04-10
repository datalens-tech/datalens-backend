from marshmallow import fields as ma_fields

from dl_constants.enums import CacheInvalidationLastResultStatus
from dl_model_tools.schema.base import BaseSchema


class CacheInvalidationLastResultErrorResponseSchema(BaseSchema):
    code = ma_fields.String(required=True)
    message = ma_fields.String(required=True, allow_none=True)
    details = ma_fields.Dict(load_default=dict)
    debug = ma_fields.Dict(load_default=dict)


class CacheInvalidationLastResultResponseSchema(BaseSchema):
    status = ma_fields.Enum(CacheInvalidationLastResultStatus, required=True)
    last_result = ma_fields.String(required=True, allow_none=True)
    timestamp = ma_fields.String(required=True, allow_none=True)
    last_result_error = ma_fields.Nested(
        CacheInvalidationLastResultErrorResponseSchema,
        required=True,
        allow_none=True,
    )
