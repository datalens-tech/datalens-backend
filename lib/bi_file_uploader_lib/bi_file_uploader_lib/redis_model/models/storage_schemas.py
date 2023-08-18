import csv
from typing import Any, Type

from marshmallow import fields, post_load, EXCLUDE
from marshmallow_oneofschema import OneOfSchema
from marshmallow_enum import EnumField

from bi_constants.enums import FileProcessingStatus
from bi_core.us_manager.storage_schemas.base_types import SchemaColumnStorageSchema

from bi_file_uploader_lib.enums import ErrorLevel, FileType, CSVEncoding, RenameTenantStatus
from bi_file_uploader_lib.redis_model.base import BaseSchema, BaseModelSchema
from bi_file_uploader_lib.redis_model.models import (
    DataFile,
    DataSource,
    DataSourcePreview,
    FileProcessingError,
    CSVFileSettings,
    CSVFileSourceSettings,
    GSheetsFileSourceSettings,
    GSheetsUserSourceProperties,
    GSheetsUserSourceDataSourceProperties,
    ExcelFileSourceSettings,
    RenameTenantStatusModel,
)


class CSVDialectSchema(BaseSchema):
    @post_load(pass_many=False)
    def to_object(self, data: dict[str, Any], **kwargs: Any) -> Any:
        class _Dialect(csv.Dialect):
            def __init__(self) -> None:
                for attr_name, val in data.items():
                    setattr(self, attr_name, val)
                super().__init__()

        return _Dialect()

    delimiter = fields.String()
    doublequote = fields.Boolean()
    escapechar = fields.String(allow_none=True)
    lineterminator = fields.String()
    quotechar = fields.String(allow_none=True)
    quoting = fields.Integer()
    skipinitialspace = fields.Boolean()


class FileProcessingErrorSchema(BaseSchema):
    class Meta(BaseSchema.Meta):
        target = FileProcessingError

    level = EnumField(ErrorLevel)
    message = fields.String()
    code = fields.List(fields.String())
    details = fields.Dict()


class FileTypeOneOfSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = 'file_type'

    def get_obj_type(self, obj: dict[str, Any]) -> str:
        return getattr(obj, self.type_field).name


class FileSettingsBaseSchema(BaseSchema):
    file_type = EnumField(FileType)


class CSVFileSettingsSchema(FileSettingsBaseSchema):
    class Meta(BaseSchema.Meta):
        target = CSVFileSettings

    encoding = EnumField(CSVEncoding)
    dialect = fields.Nested(CSVDialectSchema)


class ExcelFileSettingsSchema(FileSettingsBaseSchema):
    pass


class FileSettingsSchema(FileTypeOneOfSchema):
    type_schemas: dict[str, Type[FileSettingsBaseSchema]] = {
        FileType.csv.name: CSVFileSettingsSchema,
        FileType.xlsx.name: ExcelFileSettingsSchema,
    }


class FileSourceSettingsBaseSchema(BaseSchema):
    file_type = EnumField(FileType)


class CSVFileSourceSettingsSchema(FileSourceSettingsBaseSchema):
    class Meta(BaseSchema.Meta):
        target = CSVFileSourceSettings

    first_line_is_header = fields.Boolean()


class GSheetsFileSourceSettingsSchema(FileSourceSettingsBaseSchema):
    class Meta(BaseSchema.Meta):
        target = GSheetsFileSourceSettings

    first_line_is_header = fields.Boolean()
    raw_schema_header = fields.Nested(SchemaColumnStorageSchema, many=True)
    raw_schema_body = fields.Nested(SchemaColumnStorageSchema, many=True)


class ExcelFileSourceSettingsSchema(FileSourceSettingsBaseSchema):
    class Meta(BaseSchema.Meta):
        target = ExcelFileSourceSettings

    first_line_is_header = fields.Boolean()
    raw_schema_header = fields.Nested(SchemaColumnStorageSchema, many=True)
    raw_schema_body = fields.Nested(SchemaColumnStorageSchema, many=True)


class FileSourceSettingsSchema(FileTypeOneOfSchema):
    type_schemas: dict[str, Type[FileSourceSettingsBaseSchema]] = {
        FileType.csv.name: CSVFileSourceSettingsSchema,
        FileType.gsheets.name: GSheetsFileSourceSettingsSchema,
        FileType.xlsx.name: ExcelFileSourceSettingsSchema,
    }


class UserSourcePropertiesBaseSchema(BaseSchema):
    file_type = EnumField(FileType)


class GSheetsUserSourcePropertiesSchema(UserSourcePropertiesBaseSchema):
    class Meta(BaseSchema.Meta):
        target = GSheetsUserSourceProperties

    refresh_token = fields.String(allow_none=True)
    spreadsheet_id = fields.String()


class UserSourcePropertiesSchema(FileTypeOneOfSchema):
    type_schemas: dict[str, Type[UserSourcePropertiesBaseSchema]] = {
        FileType.gsheets.name: GSheetsUserSourcePropertiesSchema,
    }


class UserSourceDataSourcePropertiesBaseSchema(BaseSchema):
    file_type = EnumField(FileType)


class GSheetsUserSourceDataSourcePropertiesSchema(UserSourceDataSourcePropertiesBaseSchema):
    class Meta(BaseSchema.Meta):
        target = GSheetsUserSourceDataSourceProperties

    sheet_id = fields.Integer()


class UserSourceDataSourcePropertiesSchema(FileTypeOneOfSchema):
    type_schemas: dict[str, Type[UserSourceDataSourcePropertiesBaseSchema]] = {
        FileType.gsheets.name: GSheetsUserSourceDataSourcePropertiesSchema,
    }


class DataSourceSchema(BaseSchema):
    class Meta(BaseSchema.Meta):
        target = DataSource

    id = fields.String()
    s3_key = fields.String()
    preview_id = fields.String(allow_none=True)
    title = fields.String()
    raw_schema = fields.Nested(SchemaColumnStorageSchema, many=True)
    file_source_settings = fields.Nested(FileSourceSettingsSchema, allow_none=True)
    user_source_dsrc_properties = fields.Nested(UserSourceDataSourcePropertiesSchema, allow_none=True)
    status = EnumField(FileProcessingStatus)
    error = fields.Nested(FileProcessingErrorSchema, allow_none=True)


class DataFileSchema(BaseModelSchema):
    class Meta(BaseModelSchema.Meta):
        target = DataFile

    filename = fields.String()
    file_type = EnumField(FileType, allow_none=True)
    file_settings = fields.Nested(FileSettingsSchema, allow_none=True)
    user_source_properties = fields.Nested(UserSourcePropertiesSchema, allow_none=True)
    size = fields.Integer(allow_none=True)
    sources = fields.Nested(DataSourceSchema, many=True, allow_none=True)
    status = EnumField(FileProcessingStatus)
    error = fields.Nested(FileProcessingErrorSchema, allow_none=True)


class DataSourcePreviewSchema(BaseModelSchema):
    class Meta(BaseModelSchema.Meta):
        target = DataSourcePreview

    preview_data = fields.List(fields.List(fields.Raw(allow_none=True)))


class RenameTenantStatusModelSchema(BaseModelSchema):
    class Meta(BaseModelSchema.Meta):
        target = RenameTenantStatusModel

    tenant_id = fields.String()
    status = EnumField(RenameTenantStatus)
    mtime = fields.Float()
