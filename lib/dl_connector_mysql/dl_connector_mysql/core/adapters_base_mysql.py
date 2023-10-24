from __future__ import annotations

from typing import (
    Any,
    Dict,
    Optional,
)

import attr
import sqlalchemy.dialects.mysql as sa_mysql

from dl_core.db.native_type import SATypeSpec

from dl_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL


@attr.s(cmp=False)
class BaseMySQLAdapter:
    conn_type = CONNECTION_TYPE_MYSQL

    # Notes about type codes:
    # https://github.com/arnaudsj/mysql-python/blob/101e272eb7d97ed2e7191c63fe6bb58f0d85932a/MySQLdb/constants/FIELD_TYPE.py
    # https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-description.html
    #   -> mysql.connector.FieldType.desc
    # type number definitions:
    # https://github.com/mysql/mysql-server/blob/f8cdce86448a211511e8a039c62580ae16cb96f5/include/field_types.h#L57
    # some type descriptions:
    # https://github.com/mysql/mysql-server/blob/f8cdce86448a211511e8a039c62580ae16cb96f5/libmysql/libmysql.cc#L2557
    # type to 'describe' conversion (for most of the types):
    # https://github.com/mysql/mysql-server/blob/f8cdce86448a211511e8a039c62580ae16cb96f5/sql/sql_show.cc#L4721
    # in SA but not in 'describe' of the latest version:
    # not in those sources but in SA: `binary`, `boolean`, `fixed`, `integer`,
    # `nchar`, `nvarchar`, `numeric`, `varbinary`.

    _type_code_to_sa: Optional[Dict[Any, SATypeSpec]] = {
        1: sa_mysql.TINYINT,  # MYSQL_TYPE_TINY -> 'tinyint', 8-bit int  # untested
        2: sa_mysql.SMALLINT,  # MYSQL_TYPE_SHORT -> 'smallint', 16-bit int  # untested
        9: sa_mysql.MEDIUMINT,  # MYSQL_TYPE_INT24 -> 'mediumint', ?24-bit int?  # untested
        3: sa_mysql.INTEGER,  # MYSQL_TYPE_LONG -> 'int', 32-bit int  # untested
        8: sa_mysql.BIGINT,  # MYSQL_TYPE_LONGLONG -> 'bigint', 64-bit int
        4: sa_mysql.FLOAT,  # MYSQL_TYPE_FLOAT -> 'float', 32-bit float  # untested
        5: sa_mysql.DOUBLE,  # MYSQL_TYPE_DOUBLE -> 'double', 64-bit float
        # ?: sa_mysql.NUMERIC,
        0: sa_mysql.DECIMAL,  # MYSQL_TYPE_DECIMAL -> 'decimal(%d,?)'  # untested
        246: sa_mysql.DECIMAL,  # MYSQL_TYPE_NEWDECIMAL -> 'decimal(%d,%d)'
        # 6: NULL  # not in SA
        7: sa_mysql.TIMESTAMP,  # MYSQL_TYPE_TIMESTAMP -> 'timestamp'  # untested
        17: sa_mysql.TIMESTAMP,  # MYSQL_TYPE_TIMESTAMP2 -> 'timestamp'  # untested
        10: sa_mysql.DATE,  # MYSQL_TYPE_DATE -> 'date'
        14: sa_mysql.DATE,  # MYSQL_TYPE_NEWDATE -> 'date'  # /**< Internal to MySQL. Not used in protocol */
        11: sa_mysql.TIME,  # MYSQL_TYPE_TIME -> 'time'
        19: sa_mysql.TIME,  # MYSQL_TYPE_TIME2 -> 'time'  # /**< Internal to MySQL. Not used in protocol */
        12: sa_mysql.DATETIME,  # MYSQL_TYPE_DATETIME -> 'datetime'
        18: sa_mysql.DATETIME,  # MYSQL_TYPE_DATETIME2 -> 'datetime'  # /**< Internal to MySQL. Not used in protocol */
        13: sa_mysql.YEAR,  # MYSQL_TYPE_YEAR -> 'year'  # untested
        16: sa_mysql.BIT,  # MYSQL_TYPE_BIT -> 'bit(%d)'  # untested
        # 20: TYPED_ARRAY  # not in SA, /**< Used for replication only */
        247: sa_mysql.ENUM,  # MYSQL_TYPE_ENUM -> 'enum'  # untested
        248: sa_mysql.SET,  # MYSQL_TYPE_SET -> 'set'  # untested
        15: sa_mysql.VARCHAR,  # MYSQL_TYPE_VARCHAR -> 'varchar(%u)' | 'varchar(%u(bytes))'  # untested
        # MYSQL_TYPE_TINY_BLOB -> 'tinyblob' | 'tinytext' ->
        249: sa_mysql.TINYTEXT,  # or sa_mysql.TINYBLOB  # untested
        # MYSQL_TYPE_MEDIUM_BLOB -> 'mediumblob' | 'mediumtext' ->
        250: sa_mysql.MEDIUMTEXT,  # or sa_mysql.MEDIUMBLOB  # untested
        # MYSQL_TYPE_LONG_BLOB -> 'longblob' | 'longtext' ->
        251: sa_mysql.LONGTEXT,  # or sa_mysql.LONGBLOB  # untested
        # MYSQL_TYPE_BLOB -> 'tinyblob' | 'mediumblob' | 'longblob' | 'blob' | 'text' -> ...
        252: sa_mysql.TEXT,  # or either of the others  # untested
        253: sa_mysql.TEXT,  # MYSQL_TYPE_VAR_STRING -> 'varchar(%u)' | 'varchar(%u(bytes))'
        254: sa_mysql.CHAR,  # MYSQL_TYPE_STRING -> 'char(%d)' | 'char(%d(bytes)'  # untested
        245: sa_mysql.JSON,  # MYSQL_TYPE_JSON -> 'json'  # untested
        # 255: GEOMETRY  # not in SA
        # Another important note: these types don't designate *arrays* by
        # themselves. Might have to look into the data for that.
    }
