import sqlalchemy as sa
from sqlalchemy.dialects import mysql as my_types

from dl_constants.enums import BIType

from dl_core.db.conversion_base import TypeTransformer, make_native_type

from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL


class MySQLTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_MYSQL
    native_to_user_map = {
        **{
            make_native_type(CONNECTION_TYPE_MYSQL, t): BIType.integer  # type: ignore  # TODO: fix
            for t in (
                my_types.TINYINT, my_types.SMALLINT, my_types.MEDIUMINT,
                my_types.INTEGER, my_types.BIGINT
            )
        },
        **{
            make_native_type(CONNECTION_TYPE_MYSQL, t): BIType.float  # type: ignore  # TODO: fix
            for t in (
                my_types.FLOAT, my_types.DOUBLE, my_types.NUMERIC, my_types.DECIMAL,
            )
        },
        make_native_type(CONNECTION_TYPE_MYSQL, my_types.BIT): BIType.boolean,
        **{make_native_type(CONNECTION_TYPE_MYSQL, t): BIType.string for t in (
            my_types.TINYBLOB, my_types.BLOB, my_types.BINARY, my_types.VARBINARY,
            my_types.CHAR, my_types.VARCHAR, my_types.TINYTEXT, my_types.TEXT
        )},
        make_native_type(CONNECTION_TYPE_MYSQL, my_types.DATE): BIType.date,
        **{
            make_native_type(CONNECTION_TYPE_MYSQL, t): BIType.genericdatetime  # type: ignore  # TODO: fix
            for t in (my_types.DATETIME, my_types.TIMESTAMP)
        },
        make_native_type(CONNECTION_TYPE_MYSQL, my_types.ENUM): BIType.string,
        make_native_type(CONNECTION_TYPE_MYSQL, sa.sql.sqltypes.NullType): BIType.unsupported,
    }
    user_to_native_map = {
        BIType.integer: make_native_type(CONNECTION_TYPE_MYSQL, my_types.BIGINT),
        BIType.float: make_native_type(CONNECTION_TYPE_MYSQL, my_types.DOUBLE),
        BIType.boolean: make_native_type(CONNECTION_TYPE_MYSQL, my_types.BIT),
        BIType.string: make_native_type(CONNECTION_TYPE_MYSQL, my_types.VARCHAR),
        BIType.date: make_native_type(CONNECTION_TYPE_MYSQL, my_types.DATE),
        BIType.datetime: make_native_type(CONNECTION_TYPE_MYSQL, my_types.DATETIME),
        BIType.genericdatetime: make_native_type(CONNECTION_TYPE_MYSQL, my_types.DATETIME),
        BIType.geopoint: make_native_type(CONNECTION_TYPE_MYSQL, my_types.VARCHAR),
        BIType.geopolygon: make_native_type(CONNECTION_TYPE_MYSQL, my_types.VARCHAR),
        BIType.uuid: make_native_type(CONNECTION_TYPE_MYSQL, my_types.VARCHAR),
        BIType.markup: make_native_type(CONNECTION_TYPE_MYSQL, my_types.VARCHAR),
        BIType.unsupported: make_native_type(CONNECTION_TYPE_MYSQL, sa.sql.sqltypes.NullType),
    }
