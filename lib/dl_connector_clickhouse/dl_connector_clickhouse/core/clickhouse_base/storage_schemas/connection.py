from typing import (
    Generic,
    TypeVar,
)

from marshmallow import fields as ma_fields

from dl_core.us_manager.storage_schemas.connection import ConnectionSQLDataStorageSchema

from dl_connector_clickhouse.core.clickhouse_base.us_connection import ConnectionClickhouseBase


_CH_CONN_DATA_TV = TypeVar("_CH_CONN_DATA_TV", bound=ConnectionClickhouseBase.DataModel)


class ConnectionClickHouseBaseDataStorageSchema(
    ConnectionSQLDataStorageSchema[_CH_CONN_DATA_TV],
    Generic[_CH_CONN_DATA_TV],
):
    secure = ma_fields.Boolean(allow_none=False, required=False, load_default=False, dump_default=False)
    ssl_ca = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    endpoint = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    cluster_name = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    max_execution_time = ma_fields.Integer(required=False, allow_none=True, load_default=None, dump_default=None)
    readonly = ma_fields.Integer(required=False, load_default=2, dump_default=2)
