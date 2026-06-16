from __future__ import annotations

from marshmallow import fields as ma_fields

from dl_api_lib.schemas.action import ActionSchema
from dl_api_lib.schemas.dataset_base import (
    DatasetContentInternalSchema,
    OptionsMixin,
)
from dl_constants.enums import ComponentErrorLevel
from dl_core.marshmallow import ErrorCodeField
from dl_model_tools.schema.base import BaseSchema


class DatasetValidationQuerySchema(BaseSchema):
    rev_id = ma_fields.String()


class DatasetValidationSchema(BaseSchema):
    """
    Full dataset configuration with a batch of updates that should be applied consecutively to the configuration.
    Validation errors are returned only for the final version (after all of the updates are applied)
    """

    dataset = ma_fields.Nested(DatasetContentInternalSchema, required=False)
    updates = ma_fields.Nested(ActionSchema, many=True, required=False)


class FieldErrorListSchema(BaseSchema):
    class FieldErrorSchema(BaseSchema):
        message = ma_fields.String()
        code = ErrorCodeField()
        column = ma_fields.Integer()
        row = ma_fields.Integer()
        level = ma_fields.Enum(ComponentErrorLevel)
        token = ma_fields.String()

    title = ma_fields.String()
    guid = ma_fields.String()
    errors = ma_fields.Nested(FieldErrorSchema, many=True)


class DatasetValidationResponseSchema(OptionsMixin):
    dataset = ma_fields.Nested(DatasetContentInternalSchema, required=False)
    message = ma_fields.String()
    code = ma_fields.String()
    dataset_errors = ma_fields.List(ma_fields.String())  # TODO: Remove
    rev_id = ma_fields.String(data_key="revId", allow_none=True, dump_default=None, load_default=None)
    saved_id = ma_fields.String(data_key="savedId", allow_none=True, dump_default=None, load_default=None)
    published_id = ma_fields.String(data_key="publishedId", allow_none=True, dump_default=None, load_default=None)


class FieldValidationSchema(BaseSchema):
    class FieldFormulaSchema(BaseSchema):
        title = ma_fields.String(required=True)
        guid = ma_fields.String()
        formula = ma_fields.String(load_default="")

    dataset = ma_fields.Nested(DatasetContentInternalSchema, required=False)
    field = ma_fields.Nested(FieldFormulaSchema)


class FieldValidationResponseSchema(BaseSchema):
    message = ma_fields.String()
    code = ma_fields.String()
    # errors
    field_errors = ma_fields.Nested(FieldErrorListSchema, many=True)
