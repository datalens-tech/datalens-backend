import csv
from typing import Any

from marshmallow import (
    EXCLUDE,
    fields,
    post_load,
)
from marshmallow_oneofschema import OneOfSchema

from dl_constants.enums import FileProcessingStatus
from dl_core.us_manager.storage_schemas.base_types import SchemaColumnStorageSchema
from dl_file_uploader_lib.enums import (
    CSVEncoding,
    ErrorLevel,
    FileType,
    RenameTenantStatus,
)
from dl_file_uploader_lib.redis_model.base import (
    BaseModelSchema,
    BaseSchema,
)
from dl_file_uploader_lib.redis_model.models import (
    CSVFileSettings,
    CSVFileSourceSettings,
    DataFile,
    DataSource,
    DataSourcePreview,
    ExcelFileSourceSettings,
    FileProcessingError,
    GSheetsFileSourceSettings,
    GSheetsUserSourceDataSourceProperties,
    GSheetsUserSourceProperties,
    RenameTenantStatusModel,
    YaDocsFileSourceSettings,
    YaDocsUserSourceDataSourceProperties,
    YaDocsUserSourceProperties,
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

    level = fields.Enum(ErrorLevel)
    message = fields.String()
    code = fields.List(fields.String())
    details = fields.Dict()


class FileTypeOneOfSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = "file_type"

    def get_obj_type(self, obj: dict[str, Any]) -> str:
        return getattr(obj, self.type_field).name


class FileSettingsBaseSchema(BaseSchema):
    file_type = fields.Enum(FileType)


class CSVFileSettingsSchema(FileSettingsBaseSchema):
    class Meta(BaseSchema.Meta):
        target = CSVFileSettings

    encoding = fields.Enum(CSVEncoding)
    dialect = fields.Nested(CSVDialectSchema)


class ExcelFileSettingsSchema(FileSettingsBaseSchema):
    pass


class FileSettingsSchema(FileTypeOneOfSchema):
    type_schemas: dict[str, type[FileSettingsBaseSchema]] = {  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "dict[str, type[FileSettingsBaseSchema]]", base class "OneOfSchema" defined the type as "dict[str, type[Schema]]")  [assignment]
        FileType.csv.name: CSVFileSettingsSchema,
        FileType.xlsx.name: ExcelFileSettingsSchema,
    }


class FileSourceSettingsBaseSchema(BaseSchema):
    file_type = fields.Enum(FileType)


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


class YaDocsFileSourceSettingsSchema(FileSourceSettingsBaseSchema):
    class Meta(BaseSchema.Meta):
        target = YaDocsFileSourceSettings

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
    type_schemas: dict[str, type[FileSourceSettingsBaseSchema]] = {  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "dict[str, type[FileSourceSettingsBaseSchema]]", base class "OneOfSchema" defined the type as "dict[str, type[Schema]]")  [assignment]
        FileType.csv.name: CSVFileSourceSettingsSchema,
        FileType.gsheets.name: GSheetsFileSourceSettingsSchema,
        FileType.xlsx.name: ExcelFileSourceSettingsSchema,
        FileType.yadocs.name: YaDocsFileSourceSettingsSchema,
    }


class UserSourcePropertiesBaseSchema(BaseSchema):
    file_type = fields.Enum(FileType)


class GSheetsUserSourcePropertiesSchema(UserSourcePropertiesBaseSchema):
    class Meta(BaseSchema.Meta):
        target = GSheetsUserSourceProperties

    refresh_token = fields.String(allow_none=True)
    spreadsheet_id = fields.String()


class YaDocsUserSourcePropertiesSchema(UserSourcePropertiesBaseSchema):
    class Meta(BaseSchema.Meta):
        target = YaDocsUserSourceProperties

    private_path = fields.String(allow_none=True)
    public_link = fields.String(allow_none=True)
    oauth_token = fields.String(allow_none=True)


class UserSourcePropertiesSchema(FileTypeOneOfSchema):
    type_schemas: dict[str, type[UserSourcePropertiesBaseSchema]] = {  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "dict[str, type[UserSourcePropertiesBaseSchema]]", base class "OneOfSchema" defined the type as "dict[str, type[Schema]]")  [assignment]
        FileType.gsheets.name: GSheetsUserSourcePropertiesSchema,
        FileType.yadocs.name: YaDocsUserSourcePropertiesSchema,
    }


class UserSourceDataSourcePropertiesBaseSchema(BaseSchema):
    file_type = fields.Enum(FileType)


class GSheetsUserSourceDataSourcePropertiesSchema(UserSourceDataSourcePropertiesBaseSchema):
    class Meta(BaseSchema.Meta):
        target = GSheetsUserSourceDataSourceProperties

    sheet_id = fields.Integer()


class YaDocsUserSourceDataSourcePropertiesSchema(UserSourceDataSourcePropertiesBaseSchema):
    class Meta(BaseSchema.Meta):
        target = YaDocsUserSourceDataSourceProperties

    sheet_id = fields.String()


class UserSourceDataSourcePropertiesSchema(FileTypeOneOfSchema):
    type_schemas: dict[str, type[UserSourceDataSourcePropertiesBaseSchema]] = {  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "dict[str, type[UserSourceDataSourcePropertiesBaseSchema]]", base class "OneOfSchema" defined the type as "dict[str, type[Schema]]")  [assignment]
        FileType.gsheets.name: GSheetsUserSourceDataSourcePropertiesSchema,
        FileType.yadocs.name: YaDocsUserSourceDataSourcePropertiesSchema,
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
    status = fields.Enum(FileProcessingStatus)
    error = fields.Nested(FileProcessingErrorSchema, allow_none=True)


class DataFileSchema(BaseModelSchema):
    class Meta(BaseModelSchema.Meta):
        target = DataFile

    s3_key = fields.String(load_default=None, dump_default=None)  # TODO remove defaults after transition
    filename = fields.String()
    file_type = fields.Enum(FileType, allow_none=True)
    file_settings = fields.Nested(FileSettingsSchema, allow_none=True)
    user_source_properties = fields.Nested(UserSourcePropertiesSchema, allow_none=True)
    size = fields.Integer(allow_none=True)
    sources = fields.Nested(DataSourceSchema, many=True, allow_none=True)
    status = fields.Enum(FileProcessingStatus)
    error = fields.Nested(FileProcessingErrorSchema, allow_none=True)


class DataSourcePreviewSchema(BaseModelSchema):
    class Meta(BaseModelSchema.Meta):
        target = DataSourcePreview

    preview_data = fields.List(fields.List(fields.Raw(allow_none=True)))


class RenameTenantStatusModelSchema(BaseModelSchema):
    class Meta(BaseModelSchema.Meta):
        target = RenameTenantStatusModel

    tenant_id = fields.String()
    status = fields.Enum(RenameTenantStatus)
    mtime = fields.Float()
