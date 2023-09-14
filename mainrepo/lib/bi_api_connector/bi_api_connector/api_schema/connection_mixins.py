from __future__ import annotations

import marshmallow
from marshmallow import fields as ma_fields

from bi_api_connector.api_schema.extras import FieldExtra
from bi_constants.enums import RawSQLLevel
import bi_core.marshmallow as core_ma_fields


class RawSQLLevelMixin(marshmallow.Schema):
    raw_sql_level = ma_fields.Enum(
        RawSQLLevel,
        attribute="data.raw_sql_level",
        required=False,
        allow_none=False,
        load_default=RawSQLLevel.off,
        dump_default=RawSQLLevel.off,
        bi_extra=FieldExtra(editable=True),
    )


class DataExportForbiddenMixin(marshmallow.Schema):
    data_export_forbidden = core_ma_fields.OnOffField(
        attribute="data.data_export_forbidden",
        required=False,
        load_default=False,
        dump_default="off",
        bi_extra=FieldExtra(editable=True),
    )
