from marshmallow import fields

from dl_core.us_manager.storage_schemas.connection import BaseConnectionDataStorageSchema

from dl_connector_bundle_chs3.chs3_base.core.storage_schemas.connection import (
    BaseFileConnectionDataStorageSchema,
    BaseFileConnectionSourceStorageSchema,
)
from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection


class YaDocsFileConnectionSourceStorageSchema(BaseFileConnectionSourceStorageSchema):
    TARGET_CLS = YaDocsFileS3Connection.FileDataSource

    public_link = fields.String(allow_none=True, load_default=None)
    private_path = fields.String(allow_none=True, load_default=None)
    first_line_is_header = fields.Boolean(allow_none=True, load_default=None)
    sheet_id = fields.String(allow_none=True, load_default=None)
    data_updated_at = fields.DateTime()


class YaDocsFileConnectionDataStorageSchema(
    BaseFileConnectionDataStorageSchema,
    BaseConnectionDataStorageSchema[YaDocsFileS3Connection.DataModel],
):
    TARGET_CLS = YaDocsFileS3Connection.DataModel

    sources = fields.Nested(YaDocsFileConnectionSourceStorageSchema, many=True)
    oauth_token = fields.String(allow_none=True, load_default=None)
    refresh_enabled = fields.Boolean()
