from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import ConnectionBaseDataStorageSchema

from bi_connector_metrica.core.us_connection import MetrikaApiConnection, AppMetricaApiConnection


class ConnectionMetrikaApiDataStorageSchema(
        ConnectionBaseDataStorageSchema[MetrikaApiConnection.DataModel]
):
    TARGET_CLS = MetrikaApiConnection.DataModel

    token = ma_fields.String(required=True, allow_none=False)
    counter_id = ma_fields.String(required=True, allow_none=False)
    counter_creation_date = ma_fields.Date(required=False, allow_none=True, load_default=None, dump_default=None)
    accuracy = ma_fields.Float(required=False, allow_none=True, load_default=None, dump_default=None)


class ConnectionAppMetricaApiDataStorageSchema(ConnectionMetrikaApiDataStorageSchema):
    TARGET_CLS = AppMetricaApiConnection.DataModel
