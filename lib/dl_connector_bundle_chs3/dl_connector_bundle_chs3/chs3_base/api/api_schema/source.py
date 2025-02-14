from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Optional,
    Type,
    final,
)

import marshmallow as ma
from marshmallow import (
    RAISE,
    Schema,
    fields,
    post_load,
    pre_load,
)

from dl_api_connector.api_schema.extras import (
    CreateMode,
    FieldExtra,
    ImportMode,
)
from dl_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
)
from dl_constants.enums import (
    FileProcessingStatus,
    UserDataType,
)
from dl_model_tools.schema.base import BaseSchema

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection


class RawSchemaColumnSchema(Schema):
    name = fields.String()
    title = fields.String()
    user_type = fields.Enum(UserDataType)


class BaseFileSourceSchema(Schema):
    CTX_KEY_OPERATIONS_MODE: ClassVar[str] = "operations_mode"

    class Meta:
        unknown = RAISE

        target: Type[BaseFileS3Connection.FileDataSource]

    @post_load(pass_many=False)
    def post_load(self, data: dict[str, Any], **kwargs: Any) -> BaseFileS3Connection.FileDataSource:
        return self.Meta.target(**data)

    @property
    def operations_mode(self) -> Optional[CreateMode | ImportMode]:
        return self.context.get(self.CTX_KEY_OPERATIONS_MODE)

    @final
    def delete_unknown_fields(self, data: dict[str, Any]) -> dict[str, Any]:
        cleaned_data = {}
        for field_name, field_value in data.items():
            if field_name in self.fields and not self.fields[field_name].dump_only:
                cleaned_data[field_name] = field_value

        return cleaned_data

    @pre_load(pass_many=False)
    def pre_load(self, data: dict[str, Any], **_: Any) -> dict[str, Any]:
        if isinstance(self.operations_mode, ImportMode):
            return self.delete_unknown_fields(data)
        return data

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
