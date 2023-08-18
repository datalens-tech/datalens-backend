from __future__ import annotations

from marshmallow_enum import EnumField

from bi_connector_oracle.core.constants import OracleDbNameType
from bi_connector_oracle.core.us_connection import ConnectionSQLOracle

from bi_api_connector.api_schema.connection_base import ConnectionMetaMixin
from bi_api_connector.api_schema.connection_mixins import RawSQLLevelMixin, DataExportForbiddenMixin
from bi_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema
from bi_api_connector.api_schema.extras import FieldExtra


class OracleConnectionSchema(ConnectionMetaMixin, RawSQLLevelMixin,
                             DataExportForbiddenMixin, ClassicSQLConnectionSchema):
    TARGET_CLS = ConnectionSQLOracle
    ALLOW_MULTI_HOST = True

    db_connect_method = EnumField(
        OracleDbNameType, attribute='data.db_name_type',
        required=True, bi_extra=FieldExtra(editable=True)
    )
