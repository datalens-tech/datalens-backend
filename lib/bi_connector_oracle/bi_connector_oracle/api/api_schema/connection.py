from __future__ import annotations

from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema
from dl_api_connector.api_schema.extras import FieldExtra

from bi_connector_oracle.core.constants import OracleDbNameType
from bi_connector_oracle.core.us_connection import ConnectionSQLOracle


class OracleConnectionSchema(
    ConnectionMetaMixin, RawSQLLevelMixin, DataExportForbiddenMixin, ClassicSQLConnectionSchema
):
    TARGET_CLS = ConnectionSQLOracle
    ALLOW_MULTI_HOST = True

    db_connect_method = ma_fields.Enum(
        OracleDbNameType,
        attribute="data.db_name_type",
        required=True,
        bi_extra=FieldExtra(editable=True),
    )
