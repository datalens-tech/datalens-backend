from __future__ import annotations

from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema
from dl_api_connector.api_schema.extras import FieldExtra
import dl_core.marshmallow as core_ma_fields

from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse


class ClickHouseConnectionSchema(
    ConnectionMetaMixin, RawSQLLevelMixin, DataExportForbiddenMixin, ClassicSQLConnectionSchema
):
    TARGET_CLS = ConnectionClickhouse
    ALLOW_MULTI_HOST = True

    secure = core_ma_fields.OnOffField(attribute="data.secure", bi_extra=FieldExtra(editable=True))
    ssl_ca = core_ma_fields.Base64StringField(
        attribute="data.ssl_ca",
        bi_extra=FieldExtra(editable=True),
        required=False,
        allow_none=True,
        load_default=None,
        load_only=True,
    )
