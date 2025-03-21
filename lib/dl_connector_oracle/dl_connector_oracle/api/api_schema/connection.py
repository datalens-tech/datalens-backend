from __future__ import annotations

from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema
from dl_api_connector.api_schema.extras import FieldExtra
import dl_core.marshmallow as core_ma_fields

from dl_connector_oracle.core.constants import OracleDbNameType
from dl_connector_oracle.core.us_connection import ConnectionSQLOracle


class OracleConnectionSchema(
    ConnectionMetaMixin, RawSQLLevelMixin, DataExportForbiddenMixin, ClassicSQLConnectionSchema
):
    TARGET_CLS = ConnectionSQLOracle
    ALLOW_MULTI_HOST = True

    ssl_enable = core_ma_fields.OnOffField(
        attribute="data.ssl_enable",
        required=False,
        load_default=False,
        bi_extra=FieldExtra(editable=True),
    )
    ssl_ca = core_ma_fields.Base64StringField(
        attribute="data.ssl_ca",
        required=False,
        allow_none=True,
        load_only=True,
        load_default=None,
        bi_extra=FieldExtra(editable=True),
    )
    db_connect_method = ma_fields.Enum(
        OracleDbNameType,
        attribute="data.db_name_type",
        required=True,
        bi_extra=FieldExtra(editable=True),
    )
