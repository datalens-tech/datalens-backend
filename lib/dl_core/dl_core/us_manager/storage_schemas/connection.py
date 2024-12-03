from typing import (
    Generic,
    TypeVar,
)

from marshmallow import fields as ma_fields

from dl_constants.enums import RawSQLLevel
from dl_core.base_models import ConnectionDataModelBase
from dl_core.us_connection_base import (
    ConnectionBase,
    ConnectionSQL,
)
from dl_core.us_manager.storage_schemas.base import DefaultStorageSchema


_CONN_DATA_TV = TypeVar("_CONN_DATA_TV", bound=ConnectionDataModelBase)


class BaseConnectionDataStorageSchema(DefaultStorageSchema[_CONN_DATA_TV], Generic[_CONN_DATA_TV]):
    pass


_CB_DATA_TV = TypeVar("_CB_DATA_TV", bound=ConnectionBase.DataModel)


class ConnectionBaseDataStorageSchema(BaseConnectionDataStorageSchema[_CB_DATA_TV], Generic[_CB_DATA_TV]):
    table_name = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    sample_table_name = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    name = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    data_export_forbidden = ma_fields.Boolean(required=False, allow_none=False, load_default=False, dump_default=False)
    schema_version = ma_fields.String(required=False, allow_none=False, load_default="1", dump_default="1")


class CacheableConnectionDataSchemaMixin:
    cache_ttl_sec = ma_fields.Integer(required=False, allow_none=True, load_default=None, dump_default=None)


class SubselectConnectionDataSchemaMixin:
    raw_sql_level = ma_fields.Enum(RawSQLLevel, dump_default=RawSQLLevel.off)


_SQL_CONN_DATA_TV = TypeVar("_SQL_CONN_DATA_TV", bound=ConnectionSQL.DataModel)


class ConnectionSQLDataStorageSchema(
    CacheableConnectionDataSchemaMixin,
    SubselectConnectionDataSchemaMixin,
    ConnectionBaseDataStorageSchema[_SQL_CONN_DATA_TV],
    Generic[_SQL_CONN_DATA_TV],
):
    host = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    port = ma_fields.Integer(required=False, allow_none=True, load_default=None, dump_default=None)
    db_name = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    username = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
    password = ma_fields.String(required=False, allow_none=True, load_default=None, dump_default=None)
