from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_base_fields import secret_string_field
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema
from dl_api_connector.api_schema.extras import FieldExtra

from dl_connector_trino.core.constants import TrinoAuthType
from dl_connector_trino.core.us_connection import ConnectionTrino


class TrinoConnectionSchema(
    ConnectionMetaMixin,
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
    ClassicSQLConnectionSchema,
):
    TARGET_CLS = ConnectionTrino

    password = secret_string_field(
        attribute="data.password",
        required=False,
        allow_none=True,
    )
    auth_type = ma_fields.Enum(
        TrinoAuthType,
        attribute="data.auth_type",
        required=False,
        default=TrinoAuthType.NONE,
        bi_extra=FieldExtra(editable=True),
    )
