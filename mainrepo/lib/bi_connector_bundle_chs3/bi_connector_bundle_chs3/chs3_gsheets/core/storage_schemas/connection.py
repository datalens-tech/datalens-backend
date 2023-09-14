from marshmallow import fields

from bi_core.us_manager.storage_schemas.connection import BaseConnectionDataStorageSchema

from bi_connector_bundle_chs3.chs3_base.core.storage_schemas.connection import (
    BaseFileConnectionDataStorageSchema,
    BaseFileConnectionSourceStorageSchema,
)
from bi_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection


class GSheetsFileConnectionSourceStorageSchema(BaseFileConnectionSourceStorageSchema):
    TARGET_CLS = GSheetsFileS3Connection.FileDataSource

    spreadsheet_id = fields.String(allow_none=True, load_default=None)
    sheet_id = fields.Integer(allow_none=True, load_default=None)
    first_line_is_header = fields.Boolean(allow_none=True, load_default=None)
    data_updated_at = fields.DateTime()


class GSheetsFileConnectionDataStorageSchema(
    BaseFileConnectionDataStorageSchema,
    BaseConnectionDataStorageSchema[GSheetsFileS3Connection.DataModel],
):
    TARGET_CLS = GSheetsFileS3Connection.DataModel

    sources = fields.Nested(GSheetsFileConnectionSourceStorageSchema, many=True)
    refresh_token = fields.String(load_default=None)
    refresh_enabled = fields.Boolean()
