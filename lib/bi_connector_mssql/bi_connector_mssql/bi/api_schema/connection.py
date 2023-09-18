from __future__ import annotations

from bi_connector_mssql.core.us_connection import ConnectionMSSQL

from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_mixins import RawSQLLevelMixin, DataExportForbiddenMixin
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema


class MSSQLConnectionSchema(ConnectionMetaMixin, RawSQLLevelMixin, DataExportForbiddenMixin,
                            ClassicSQLConnectionSchema):
    TARGET_CLS = ConnectionMSSQL
    ALLOW_MULTI_HOST = True
