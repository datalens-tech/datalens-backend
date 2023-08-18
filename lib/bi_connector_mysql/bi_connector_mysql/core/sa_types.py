from __future__ import annotations

from sqlalchemy.dialects import mysql as my_types

from bi_core.db.sa_types_base import (
    make_native_type, simple_instantiator, lengthed_instantiator,
)

from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL


SQLALCHEMY_MYSQL_BASE_TYPES = (
    my_types.TINYINT, my_types.SMALLINT, my_types.MEDIUMINT,
    my_types.INTEGER, my_types.BIGINT,
    my_types.FLOAT, my_types.DOUBLE, my_types.NUMERIC, my_types.DECIMAL,
    my_types.TINYTEXT,
    my_types.DATE, my_types.DATETIME, my_types.TIMESTAMP,
    # Lengthed but length might be optional:
    my_types.BIT, my_types.TEXT,
    my_types.BLOB, my_types.TINYBLOB, my_types.MEDIUMBLOB, my_types.LONGBLOB,
)
SQLALCHEMY_MYSQL_LENGTHED_TYPES = (
    my_types.CHAR, my_types.VARCHAR,
    my_types.NCHAR, my_types.NVARCHAR,
    my_types.BINARY, my_types.VARBINARY,
)
SQLALCHEMY_MYSQL_TYPES = {
    **{make_native_type(CONNECTION_TYPE_MYSQL, typecls): simple_instantiator(typecls)
       for typecls in SQLALCHEMY_MYSQL_BASE_TYPES},
    **{make_native_type(CONNECTION_TYPE_MYSQL, typecls): lengthed_instantiator(typecls)
       for typecls in SQLALCHEMY_MYSQL_LENGTHED_TYPES},
}
