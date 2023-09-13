from __future__ import annotations

from bi_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse

from bi_api_connector.api_schema.extras import FieldExtra
from bi_api_connector.api_schema.connection_base import ConnectionMetaMixin
from bi_api_connector.api_schema.connection_mixins import RawSQLLevelMixin, DataExportForbiddenMixin
from bi_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema
import bi_core.marshmallow as core_ma_fields


class ClickHouseConnectionSchema(ConnectionMetaMixin, RawSQLLevelMixin, DataExportForbiddenMixin,
                                 ClassicSQLConnectionSchema):
    TARGET_CLS = ConnectionClickhouse
    ALLOW_MULTI_HOST = True

    secure = core_ma_fields.OnOffField(attribute='data.secure', bi_extra=FieldExtra(editable=True))
    ssl_ca = core_ma_fields.Base64StringField(
        attribute='data.ssl_ca',
        bi_extra=FieldExtra(editable=True),
        required=False,
        allow_none=True,
        load_default=None,
        load_only=True,
    )
