from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import (
    CacheableConnectionDataSchemaMixin,
    ConnectionBaseDataStorageSchema,
)

from bi_connector_monitoring.core.us_connection import MonitoringConnection


class MonitoringConnectionDataStorageSchema(
    ConnectionBaseDataStorageSchema[MonitoringConnection.DataModel],
    CacheableConnectionDataSchemaMixin,
):
    TARGET_CLS = MonitoringConnection.DataModel

    service_account_id = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)
    folder_id = ma_fields.String(required=False, allow_none=True, dump_default=None, load_default=None)
