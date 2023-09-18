from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.source_base import (
    SimpleDataSourceSchema,
    SimpleDataSourceTemplateSchema,
)
from dl_model_tools.schema.base import BaseSchema


class SnowFlakeTableParametersSchema(BaseSchema):
    db_name = ma_fields.String(allow_none=True)
    schema = ma_fields.String(allow_none=True)
    table_name = ma_fields.String(allow_none=True)


class SnowFlakeTableDataSourceSchema(SimpleDataSourceSchema):
    parameters = ma_fields.Nested(SnowFlakeTableParametersSchema)


class SnowFlakeTableDataSourceTemplateSchema(SimpleDataSourceTemplateSchema):
    parameters = ma_fields.Nested(SnowFlakeTableParametersSchema)
