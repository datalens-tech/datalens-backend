from __future__ import annotations

from typing import (
    Any,
    Type,
)

import marshmallow as ma
from marshmallow import (
    RAISE,
    Schema,
    fields,
    post_load,
)

from bi_api_connector.api_schema.extras import FieldExtra
from bi_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
)
from bi_constants.enums import (
    BIType,
    FileProcessingStatus,
)
from bi_model_tools.schema.base import BaseSchema

from bi_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection


class RawSchemaColumnSchema(Schema):
    name = fields.String()
    title = fields.String()
    user_type = fields.Enum(BIType)


class BaseFileSourceSchema(Schema):
    class Meta:
        unknown = RAISE

        target: Type[BaseFileS3Connection.FileDataSource]

    @post_load(pass_many=False)
    def to_object(self, data: dict[str, Any], **kwargs: Any) -> BaseFileS3Connection.FileDataSource:
        return self.Meta.target(**data)

    id = fields.String()
    file_id = fields.String(load_default=None)
    title = fields.String(bi_extra=FieldExtra(editable=True))
    status = fields.Enum(FileProcessingStatus, dump_only=True)
    raw_schema = fields.Nested(
        RawSchemaColumnSchema,
        many=True,
        dump_only=True,
    )
    preview = fields.List(fields.List(fields.Raw), dump_only=True, dump_default=None)
    # meta_info = {"row_num": 100500, "size": 256, "file_name": 'file_name.csv', }


class ReplaceFileSourceSchema(Schema):
    class Meta:
        unknown = RAISE

    old_source_id = fields.String()
    new_source_id = fields.String()


class BaseFileS3DataSourceParametersSchema(BaseSchema):
    origin_source_id = ma.fields.String(allow_none=True, load_default=None)


class BaseFileS3DataSourceSchema(SQLDataSourceSchema):
    parameters = ma.fields.Nested(BaseFileS3DataSourceParametersSchema)


class BaseFileS3DataSourceTemplateSchema(SQLDataSourceTemplateSchema):
    parameters = ma.fields.Nested(BaseFileS3DataSourceParametersSchema)
