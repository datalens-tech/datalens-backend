from __future__ import annotations

import marshmallow
from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base_fields import cache_ttl_field
from dl_api_connector.api_schema.extras import FieldExtra
from dl_constants.enums import RawSQLLevel
import dl_core.marshmallow as core_ma_fields


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


class QueryCacheMixin(marshmallow.Schema):
    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")

    cache_invalidation_enabled = core_ma_fields.OnOffField(
        attribute="data.cache_invalidation_enabled",
        required=False,
        load_default=False,
        dump_default="off",
        bi_extra=FieldExtra(editable=True),
    )
    cache_invalidation_throttling_interval_sec = ma_fields.Integer(
        attribute="data.cache_invalidation_throttling_interval_sec",
        required=False,
        load_default=10,
        dump_default=10,
        bi_extra=FieldExtra(editable=True),
    )
