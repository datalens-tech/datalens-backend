from dl_file_uploader_lib.redis_model.base import register_redis_model_storage_schema

from .models import (
    CSVFileSettings,
    CSVFileSourceSettings,
    DataFile,
    DataFileError,
    DataSource,
    DataSourcePreview,
    EmptySourcesError,
    ExcelFileSourceSettings,
    FileProcessingError,
    FileSettings,
    FileSourceSettings,
    GSheetsFileSourceSettings,
    GSheetsUserSourceDataSourceProperties,
    GSheetsUserSourceProperties,
    PreviewSet,
    RenameTenantStatusModel,
    SourceNotFoundError,
    SpreadsheetFileSourceSettings,
    YaDocsFileSourceSettings,
    YaDocsUserSourceDataSourceProperties,
    YaDocsUserSourceProperties,
)
from .storage_schemas import (
    DataFileSchema,
    DataSourcePreviewSchema,
    RenameTenantStatusModelSchema,
)


__all__ = (
    "DataFile",
    "FileSettings",
    "FileSourceSettings",
    "CSVFileSettings",
    "CSVFileSourceSettings",
    "GSheetsFileSourceSettings",
    "ExcelFileSourceSettings",
    "SpreadsheetFileSourceSettings",
    "DataSource",
    "DataSourcePreview",
    "GSheetsUserSourceProperties",
    "GSheetsUserSourceDataSourceProperties",
    "FileProcessingError",
    "DataFileError",
    "SourceNotFoundError",
    "EmptySourcesError",
    "RenameTenantStatusModel",
    "PreviewSet",
    "YaDocsFileSourceSettings",
    "YaDocsUserSourceProperties",
    "YaDocsUserSourceDataSourceProperties",
)


register_redis_model_storage_schema(DataFile, DataFileSchema)
register_redis_model_storage_schema(DataSourcePreview, DataSourcePreviewSchema)
register_redis_model_storage_schema(RenameTenantStatusModel, RenameTenantStatusModelSchema)
