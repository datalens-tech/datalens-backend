from __future__ import annotations

from typing import (
    Any,
)

import marshmallow as ma
from marshmallow_oneofschema import OneOfSchema

from dl_constants.enums import (
    FileProcessingStatus,
    UserDataType,
)
from dl_core.db.elements import SchemaColumn
from dl_file_uploader_api_lib.schemas.base import BaseRequestSchema
from dl_file_uploader_api_lib.schemas.errors import (
    ErrorInfoSchema,
    FileProcessingErrorApiSchema,
)
from dl_file_uploader_lib.enums import (
    CSVDelimiter,
    CSVEncoding,
    FileType,
)
from dl_type_transformer.native_type_schema import OneOfNativeTypeSchema


class SourceInfoBaseRequestSchema(BaseRequestSchema):
    file_id = ma.fields.String(required=True)
    source_id = ma.fields.String(required=True)


class SourceStatusResultSchema(ma.Schema):
    file_id = ma.fields.String()
    source_id = ma.fields.String()
    status = ma.fields.Enum(enum=FileProcessingStatus)
    errors = ma.fields.Nested(ErrorInfoSchema, many=True)
    error = ma.fields.Nested(FileProcessingErrorApiSchema, allow_none=True)
    # progress = ma.fields.Float()


class RawSchemaColumnSchema(ma.Schema):
    name = ma.fields.String()
    title = ma.fields.String(load_default=None, allow_none=True)

    native_type = ma.fields.Nested(OneOfNativeTypeSchema, allow_none=True, load_default=None)

    user_type = ma.fields.Enum(UserDataType)
    description = ma.fields.String(dump_default="", allow_none=True, load_default="")
    has_auto_aggregation = ma.fields.Boolean(dump_default=False, allow_none=True, load_default=False)
    lock_aggregation = ma.fields.Boolean(dump_default=False, allow_none=True, load_default=False)
    nullable = ma.fields.Boolean(dump_default=None, allow_none=True, load_default=None)

    @ma.post_load
    def make_column(self, data: dict, **kwargs: Any) -> SchemaColumn:
        return SchemaColumn(
            name=data["name"],
            title=data["title"],
            user_type=data["user_type"],
            native_type=data["native_type"],
            description=data.get("description", ""),
            has_auto_aggregation=data.get("has_auto_aggregation", False),
            lock_aggregation=data.get("lock_aggregation", False),
            nullable=data["nullable"],
        )


class SourceInfoRequestSchema(SourceInfoBaseRequestSchema):
    column_types = ma.fields.Nested(RawSchemaColumnSchema, many=True, load_default=[])


class FileTypeOneOfSchema(OneOfSchema):
    class Meta:
        unknown = ma.EXCLUDE

    type_field_remove = True
    type_field = "file_type"

    def get_obj_type(self, obj: dict[str, Any]) -> str:
        type_field = obj[self.type_field] if isinstance(obj, dict) else getattr(obj, self.type_field)
        assert isinstance(type_field, FileType)
        return type_field.name


class SourceInfoSchemaBase(ma.Schema):
    class RawSchemaColumnSchemaShorten(ma.Schema):
        name = ma.fields.String()
        title = ma.fields.String()
        user_type = ma.fields.Enum(UserDataType)

    source_id = ma.fields.String()
    title = ma.fields.String()
    is_valid = ma.fields.Boolean()
    raw_schema = ma.fields.Nested(RawSchemaColumnSchemaShorten, many=True)
    preview = ma.fields.List(ma.fields.List(ma.fields.Raw(allow_none=True)))
    error = ma.fields.Nested(FileProcessingErrorApiSchema, allow_none=True)


class SourceInfoSchemaGSheets(SourceInfoSchemaBase):
    sheet_id = ma.fields.Integer()
    spreadsheet_id = ma.fields.String()


class SourceInfoSchemaYaDocs(SourceInfoSchemaBase):
    sheet_id = ma.fields.String()
    private_path = ma.fields.String(allow_none=True)
    public_link = ma.fields.String(allow_none=True)


class SourceInfoSchema(FileTypeOneOfSchema):
    type_schemas: dict[str, type[SourceInfoSchemaBase]] = {  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "dict[str, type[SourceInfoSchemaBase]]", base class "OneOfSchema" defined the type as "dict[str, type[Schema]]")  [assignment]
        FileType.csv.name: SourceInfoSchemaBase,
        FileType.gsheets.name: SourceInfoSchemaGSheets,
        FileType.xlsx.name: SourceInfoSchemaBase,
        FileType.yadocs.name: SourceInfoSchemaYaDocs,
    }


class CSVSettingsSchema(ma.Schema):
    encoding = ma.fields.Enum(CSVEncoding)
    delimiter = ma.fields.Enum(CSVDelimiter)
    first_line_is_header = ma.fields.Boolean()


class CSVSettingsOptionsSchema(ma.Schema):
    encoding = ma.fields.List(ma.fields.Enum(CSVEncoding))
    delimiter = ma.fields.List(ma.fields.Enum(CSVDelimiter))


class OptionsSchema(ma.Schema):
    class ColumnsOptionsSchema(ma.Schema):
        name = ma.fields.String()
        user_type = ma.fields.List(ma.fields.Enum(UserDataType))

    data_settings = ma.fields.Nested(CSVSettingsOptionsSchema)
    columns = ma.fields.Nested(ColumnsOptionsSchema, many=True)


class SourceInfoResultSchema(ma.Schema):
    file_id = ma.fields.String()
    file_type = ma.fields.Enum(FileType)
    errors = ma.fields.Nested(ErrorInfoSchema, many=True)
    source = ma.fields.Nested(SourceInfoSchema)
    data_settings = ma.fields.Nested(CSVSettingsSchema)  # TODO: One of schema by file_type
    options = ma.fields.Nested(OptionsSchema, allow_none=True)


class SourceApplySettingsRequestSchema(SourceInfoBaseRequestSchema):
    data_settings = ma.fields.Nested(CSVSettingsSchema)
    title = ma.fields.String(required=False, load_default=None, allow_none=True)


class SourcePreviewRequestSchema(SourceInfoBaseRequestSchema):
    preview_id = ma.fields.String()
    raw_schema = ma.fields.Nested(RawSchemaColumnSchema, many=True, required=True)


class SourcePreviewResultSchema(ma.Schema):
    preview = ma.fields.List(ma.fields.List(ma.fields.Raw(allow_none=True)))


class SourceInternalParamsRequestSchema(SourceInfoBaseRequestSchema):
    raw_schema = ma.fields.Nested(RawSchemaColumnSchema, many=True, load_default=[], allow_none=True)


class SourceInternalParamsResultSchema(ma.Schema):
    preview_id = ma.fields.String()
    raw_schema = ma.fields.Nested(RawSchemaColumnSchema, many=True)
