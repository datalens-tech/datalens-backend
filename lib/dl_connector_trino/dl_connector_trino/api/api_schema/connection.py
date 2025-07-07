
from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_base_fields import secret_string_field
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema
from dl_api_connector.api_schema.extras import FieldExtra
import dl_core.marshmallow as core_ma_fields
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField

from dl_connector_trino.core.constants import TrinoAuthType
from dl_connector_trino.core.us_connection import ConnectionTrino


class TrinoConnectionSchema(
    ConnectionMetaMixin,
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
    ClassicSQLConnectionSchema,
):
    TARGET_CLS = ConnectionTrino

    auth_type = DynamicEnumField(
        TrinoAuthType,
        attribute="data.auth_type",
        required=True,
        bi_extra=FieldExtra(editable=True),
    )
    password = secret_string_field(
        attribute="data.password",
        required=False,
        allow_none=True,
    )
    jwt = secret_string_field(
        attribute="data.jwt",
        required=False,
        allow_none=True,
    )
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
