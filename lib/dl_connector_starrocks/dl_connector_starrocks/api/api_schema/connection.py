from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_base_fields import secret_string_field
from dl_api_connector.api_schema.connection_mixins import (
    DataExportForbiddenMixin,
    RawSQLLevelMixin,
)
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema
from dl_api_connector.api_schema.extras import FieldExtra
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField

from dl_connector_starrocks.core.constants import ListingSources
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
    listing_sources = DynamicEnumField(
        ListingSources,
        attribute="data.listing_sources",
        required=False,
        allow_none=False,
        dump_default=ListingSources.on,
        load_default=ListingSources.on,
        bi_extra=FieldExtra(editable=True),
    )
