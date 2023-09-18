from __future__ import annotations

from bi_connector_mysql.core.us_connection import ConnectionMySQL

from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_mixins import RawSQLLevelMixin, DataExportForbiddenMixin
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema


class MySQLConnectionSchema(
        ConnectionMetaMixin,
        DataExportForbiddenMixin,
        RawSQLLevelMixin,
        ClassicSQLConnectionSchema,
):
    TARGET_CLS = ConnectionMySQL
    ALLOW_MULTI_HOST = True
