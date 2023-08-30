from __future__ import annotations

from marshmallow import fields

from bi_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode
from bi_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL

from bi_api_connector.api_schema.extras import FieldExtra
from bi_api_connector.api_schema.connection_base import ConnectionMetaMixin
from bi_api_connector.api_schema.connection_mixins import MDBDatabaseSchemaMixin,\
    RawSQLLevelMixin, DataExportForbiddenMixin
from bi_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema
import bi_core.marshmallow as core_ma_fields


class PostgreSQLConnectionSchema(ConnectionMetaMixin, MDBDatabaseSchemaMixin, DataExportForbiddenMixin, RawSQLLevelMixin,
                                 ClassicSQLConnectionSchema):
    TARGET_CLS = ConnectionPostgreSQL
    ALLOW_MULTI_HOST = True

    enforce_collate = fields.Enum(
        PGEnforceCollateMode,
        attribute='data.enforce_collate',
        required=False,
        load_default=PGEnforceCollateMode.auto,
        dump_default=PGEnforceCollateMode.auto,
        bi_extra=FieldExtra(editable=True),
    )  # Side note: previously this fields was not declared in connection testing schemas
    ssl_enable = core_ma_fields.OnOffField(
        attribute='data.ssl_enable',
        required=False,
        load_default=False,
        bi_extra=FieldExtra(editable=True),
    )
    ssl_ca = core_ma_fields.Base64StringField(
        attribute='data.ssl_ca',
        required=False,
        allow_none=True,
        load_only=True,
        load_default=None,
        bi_extra=FieldExtra(editable=True),
    )
