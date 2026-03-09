from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_base_fields import secret_string_field
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema

from dl_connector_starrocks.core.us_connection import ConnectionStarRocks


class StarRocksConnectionSchema(
    ConnectionMetaMixin,
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
    ClassicSQLConnectionSchema,
):
    TARGET_CLS = ConnectionStarRocks
    ALLOW_MULTI_HOST = True

    password = secret_string_field(
        attribute="data.password",
        required=False,
        allow_none=True,
    )
