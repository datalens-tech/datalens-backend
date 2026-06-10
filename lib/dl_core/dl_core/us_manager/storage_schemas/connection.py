from marshmallow import fields as ma_fields

from dl_constants.enums import RawSQLLevel
from dl_core.base_models import ConnectionDataModelBase
from dl_core.us_connection_base import (
    ConnectionBase,
    ConnectionSQL,
)
from dl_core.us_manager.storage_schemas.base import DefaultStorageSchema


class BaseConnectionDataStorageSchema[CONN_DATA_TV: ConnectionDataModelBase](DefaultStorageSchema[CONN_DATA_TV]):
    data_export_forbidden = ma_fields.Boolean(required=False, allow_none=False, load_default=False, dump_default=False)


class ConnectionBaseDataStorageSchema[CB_DATA_TV: ConnectionBase.DataModel](
    BaseConnectionDataStorageSchema[CB_DATA_TV]
):
    table_name = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    sample_table_name = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    name = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    schema_version = ma_fields.String(required=False, allow_none=False, load_default="1", dump_default="1")


class CacheableConnectionDataSchemaMixin:
    cache_ttl_sec = ma_fields.Integer(required=False, allow_none=True, load_default=None, dump_default=None)

    cache_invalidation_throttling_interval_sec = ma_fields.Integer(required=False, allow_none=True, load_default=None)


class SubselectConnectionDataSchemaMixin:
    raw_sql_level = ma_fields.Enum(RawSQLLevel, dump_default=RawSQLLevel.off)


class ConnectionSQLDataStorageSchema[SQL_CONN_DATA_TV: ConnectionSQL.DataModel](
    CacheableConnectionDataSchemaMixin,
    SubselectConnectionDataSchemaMixin,
    ConnectionBaseDataStorageSchema[SQL_CONN_DATA_TV],
):
    host = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    port = ma_fields.Integer(required=False, allow_none=True, load_default=None, dump_default=None)
    db_name = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    username = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    password = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
