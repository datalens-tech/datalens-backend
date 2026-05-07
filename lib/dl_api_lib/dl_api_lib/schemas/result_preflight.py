from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.component_errors import ComponentErrorListSchema
from dl_api_lib.schemas.data import FieldsResponseFieldSchema
from dl_model_tools.schema.base import BaseSchema


class ResultPreflightDatasetResponseSchema(BaseSchema):
    component_errors = ma_fields.Nested(ComponentErrorListSchema, required=True)
    fields_ = ma_fields.Nested(
        FieldsResponseFieldSchema,
        attribute="fields",
        data_key="fields",
        many=True,
        only=("title", "guid", "data_type", "calc_mode"),
        required=False,
    )


class ResultPreflightResponseSchema(BaseSchema):
    code = ma_fields.String(required=True)
    message = ma_fields.String(required=True)
    dataset = ma_fields.Nested(ResultPreflightDatasetResponseSchema, required=True)
