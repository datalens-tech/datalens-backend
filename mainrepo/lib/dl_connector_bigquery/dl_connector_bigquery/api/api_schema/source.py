from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.source_base import (
    SimpleDataSourceSchema,
    SimpleDataSourceTemplateSchema,
)
from dl_model_tools.schema.base import BaseSchema


class BigQueryTableParametersSchema(BaseSchema):
    dataset_name = ma_fields.String(allow_none=True)
    table_name = ma_fields.String(allow_none=True)
    db_version = ma_fields.String(allow_none=True)  # FIXME


class BigQueryTableDataSourceSchema(SimpleDataSourceSchema):
    parameters = ma_fields.Nested(BigQueryTableParametersSchema)


class BigQueryTableDataSourceTemplateSchema(SimpleDataSourceTemplateSchema):
    parameters = ma_fields.Nested(BigQueryTableParametersSchema)
