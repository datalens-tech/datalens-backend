from __future__ import annotations

from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema

from bi_connector_mssql.core.us_connection import ConnectionMSSQL


class MSSQLConnectionSchema(
    ConnectionMetaMixin, RawSQLLevelMixin, DataExportForbiddenMixin, ClassicSQLConnectionSchema
):
    TARGET_CLS = ConnectionMSSQL
    ALLOW_MULTI_HOST = True
