from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField

from dl_connector_starrocks.core.constants import ListingSources
from dl_connector_starrocks.core.us_connection import ConnectionStarRocks


class ConnectionStarRocksDataStorageSchema(ConnectionSQLDataStorageSchema[ConnectionStarRocks.DataModel]):
    TARGET_CLS = ConnectionStarRocks.DataModel

    listing_sources = DynamicEnumField(
        ListingSources,
        required=False,
        allow_none=False,
        dump_default=ListingSources.on,
        load_default=ListingSources.on,
    )
