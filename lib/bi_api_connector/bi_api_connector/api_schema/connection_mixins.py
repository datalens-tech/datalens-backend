from __future__ import annotations

import marshmallow
from marshmallow import fields as ma_fields
from marshmallow_enum import EnumField

from bi_constants.enums import RawSQLLevel

from bi_api_connector.api_schema.extras import FieldExtra
import bi_core.marshmallow as core_ma_fields


class MDBDatabaseSchemaMixin(marshmallow.Schema):
    mdb_cluster_id = ma_fields.String(
        attribute='data.mdb_cluster_id', allow_none=True,
        bi_extra=FieldExtra(editable=True),  # TODO QUESTION: Editable?
    )
    mdb_folder_id = ma_fields.String(
        attribute='data.mdb_folder_id', allow_none=True,
        bi_extra=FieldExtra(editable=True),
    )


class RawSQLLevelMixin(marshmallow.Schema):
    raw_sql_level = EnumField(
        RawSQLLevel,
        attribute='data.raw_sql_level',
        required=False, allow_none=False,
        load_default=RawSQLLevel.off, dump_default=RawSQLLevel.off,
        bi_extra=FieldExtra(editable=True),
    )


class DataExportForbiddenMixin(marshmallow.Schema):
    data_export_forbidden = core_ma_fields.OnOffField(
        attribute='data.data_export_forbidden',
        required=False,
        load_default=False,
        dump_default="off",
        bi_extra=FieldExtra(editable=True),
    )
