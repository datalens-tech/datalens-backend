from marshmallow import fields as ma_fields

from bi_core.connectors.chydb.us_connection import ConnectionCHYDB
from bi_core.connectors.clickhouse_base.storage_schemas.connection import ConnectionClickHouseBaseDataStorageSchema


class ConnectionCHYDBDataStorageSchema(ConnectionClickHouseBaseDataStorageSchema[ConnectionCHYDB.DataModel]):
    TARGET_CLS = ConnectionCHYDB.DataModel

    db_name = ma_fields.String(allow_none=True, load_default=None, dump_default=None, required=False)
    username = ma_fields.String(allow_none=True, load_default=None, dump_default=None, required=False)
    password = ma_fields.String(allow_none=True, load_default=None, dump_default=None, required=False)
    cluster_name = ma_fields.String(allow_none=True, load_default=None, dump_default=None, required=False)

    token = ma_fields.String(allow_none=False, required=True)

    default_ydb_cluster = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    default_ydb_database = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
