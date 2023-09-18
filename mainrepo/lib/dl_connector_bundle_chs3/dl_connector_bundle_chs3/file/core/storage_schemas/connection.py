from marshmallow import fields

from dl_connector_bundle_chs3.chs3_base.core.storage_schemas.connection import (
    BaseFileConnectionDataStorageSchema,
    BaseFileConnectionSourceStorageSchema,
)
from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_core.us_manager.storage_schemas.connection import BaseConnectionDataStorageSchema


class FileConnectionSourceStorageSchema(BaseFileConnectionSourceStorageSchema):
    TARGET_CLS = FileS3Connection.FileDataSource


class FileConnectionDataStorageSchema(
    BaseFileConnectionDataStorageSchema,
    BaseConnectionDataStorageSchema[FileS3Connection.DataModel],
):
    TARGET_CLS = FileS3Connection.DataModel

    sources = fields.Nested(FileConnectionSourceStorageSchema, many=True)
