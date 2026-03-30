import sqlalchemy as sa
from sqlalchemy.dialects import mysql as mysql_types

from dl_constants.enums import UserDataType
from dl_type_transformer.type_transformer import (
    TypeTransformer,
    make_native_type,
)


STARROCKS_TYPES_INT = frozenset((mysql_types.TINYINT, mysql_types.SMALLINT, mysql_types.INTEGER, mysql_types.BIGINT))
STARROCKS_TYPES_FLOAT = frozenset((mysql_types.FLOAT, mysql_types.DOUBLE, mysql_types.DECIMAL))
STARROCKS_TYPES_STRING = frozenset((mysql_types.CHAR, mysql_types.VARCHAR, mysql_types.TEXT))

STARROCKS_INFO_SCHEMA_ALIASES = {
    make_native_type("int"): UserDataType.integer,
    make_native_type("tinyint"): UserDataType.boolean,
}


class StarRocksTypeTransformer(TypeTransformer):
    native_to_user_map = {
        **{make_native_type(t): UserDataType.integer for t in STARROCKS_TYPES_INT},
        **{make_native_type(t): UserDataType.float for t in STARROCKS_TYPES_FLOAT},
        make_native_type(mysql_types.BOOLEAN): UserDataType.boolean,
        **{make_native_type(t): UserDataType.string for t in STARROCKS_TYPES_STRING},
        make_native_type(mysql_types.DATE): UserDataType.date,
        make_native_type(mysql_types.DATETIME): UserDataType.genericdatetime,
        make_native_type(mysql_types.TIME): UserDataType.string,
        make_native_type(sa.sql.sqltypes.NullType): UserDataType.unsupported,
        **STARROCKS_INFO_SCHEMA_ALIASES,
    }

    user_to_native_map = {
        UserDataType.integer: make_native_type(mysql_types.BIGINT),
        UserDataType.float: make_native_type(mysql_types.DOUBLE),
        UserDataType.boolean: make_native_type(mysql_types.BOOLEAN),
        UserDataType.string: make_native_type(mysql_types.TEXT),
        UserDataType.date: make_native_type(mysql_types.DATE),
        UserDataType.datetime: make_native_type(mysql_types.DATETIME),
        UserDataType.genericdatetime: make_native_type(mysql_types.DATETIME),
        UserDataType.geopoint: make_native_type(mysql_types.TEXT),
        UserDataType.geopolygon: make_native_type(mysql_types.TEXT),
        UserDataType.markup: make_native_type(mysql_types.TEXT),
        UserDataType.unsupported: make_native_type(sa.sql.sqltypes.NullType),
    }
