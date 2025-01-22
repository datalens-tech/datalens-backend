import sqlalchemy as sa
from sqlalchemy.dialects import mysql as my_types

from dl_constants.enums import UserDataType
from dl_type_transformer.type_transformer import (
    TypeTransformer,
    make_native_type,
)


class MySQLTypeTransformer(TypeTransformer):
    native_to_user_map = {
        **{
            make_native_type(t): UserDataType.integer
            for t in (my_types.TINYINT, my_types.SMALLINT, my_types.MEDIUMINT, my_types.INTEGER, my_types.BIGINT)
        },
        **{
            make_native_type(t): UserDataType.float
            for t in (
                my_types.FLOAT,
                my_types.DOUBLE,
                my_types.NUMERIC,
                my_types.DECIMAL,
            )
        },
        make_native_type(my_types.BIT): UserDataType.boolean,
        **{
            make_native_type(t): UserDataType.string
            for t in (
                my_types.TINYBLOB,
                my_types.BLOB,
                my_types.BINARY,
                my_types.VARBINARY,
                my_types.CHAR,
                my_types.VARCHAR,
                my_types.TINYTEXT,
                my_types.TEXT,
            )
        },
        make_native_type(my_types.DATE): UserDataType.date,
        **{make_native_type(t): UserDataType.genericdatetime for t in (my_types.DATETIME, my_types.TIMESTAMP)},
        make_native_type(my_types.ENUM): UserDataType.string,
        make_native_type(sa.sql.sqltypes.NullType): UserDataType.unsupported,
    }
    user_to_native_map = {
        UserDataType.integer: make_native_type(my_types.BIGINT),
        UserDataType.float: make_native_type(my_types.DOUBLE),
        UserDataType.boolean: make_native_type(my_types.BIT),
        UserDataType.string: make_native_type(my_types.VARCHAR),
        UserDataType.date: make_native_type(my_types.DATE),
        UserDataType.datetime: make_native_type(my_types.DATETIME),
        UserDataType.genericdatetime: make_native_type(my_types.DATETIME),
        UserDataType.geopoint: make_native_type(my_types.VARCHAR),
        UserDataType.geopolygon: make_native_type(my_types.VARCHAR),
        UserDataType.uuid: make_native_type(my_types.VARCHAR),
        UserDataType.markup: make_native_type(my_types.VARCHAR),
        UserDataType.unsupported: make_native_type(sa.sql.sqltypes.NullType),
    }
