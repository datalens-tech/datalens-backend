from __future__ import annotations

from marshmallow import fields

from dl_api_connector.api_schema.connection_base import ConnectionSchema
from dl_api_connector.api_schema.connection_mixins import RawSQLLevelMixin
from dl_constants.enums import RawSQLLevel

from bi_connector_bundle_ch_frozen.ch_frozen_base.core.us_connection import ConnectionClickhouseFrozenBase


class BaseClickHouseFrozenConnectionSchema(ConnectionSchema, RawSQLLevelMixin):
    TARGET_CLS = ConnectionClickhouseFrozenBase

    raw_sql_level = fields.Enum(  # explicitly overriden to be dump-only
        RawSQLLevel,
        attribute="raw_sql_level",
        required=False,
        allow_none=False,
        load_default=RawSQLLevel.off,
        dump_default=RawSQLLevel.off,
        dump_only=True,
    )
