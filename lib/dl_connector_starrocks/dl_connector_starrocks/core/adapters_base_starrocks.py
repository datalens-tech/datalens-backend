import attr
import sqlalchemy.dialects.mysql as sa_mysql

from dl_type_transformer.native_type import SATypeSpec

from dl_connector_starrocks.core.constants import CONNECTION_TYPE_STARROCKS
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


@attr.s(cmp=False)
class BaseStarRocksAdapter:
    _target_dto: StarRocksConnTargetDTO = attr.ib()

    conn_type = CONNECTION_TYPE_STARROCKS

    # StarRocks uses MySQL wire protocol, so type codes are MySQL-compatible.
    # Integer keys are MySQL protocol field type codes from field_types.h:
    # https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
    _type_code_to_sa: dict[int, SATypeSpec] | None = {
        0: sa_mysql.DECIMAL,  # MYSQL_TYPE_DECIMAL
        1: sa_mysql.TINYINT,  # MYSQL_TYPE_TINY
        2: sa_mysql.SMALLINT,  # MYSQL_TYPE_SHORT
        3: sa_mysql.INTEGER,  # MYSQL_TYPE_LONG
        4: sa_mysql.FLOAT,  # MYSQL_TYPE_FLOAT
        5: sa_mysql.DOUBLE,  # MYSQL_TYPE_DOUBLE
        7: sa_mysql.TIMESTAMP,  # MYSQL_TYPE_TIMESTAMP
        8: sa_mysql.BIGINT,  # MYSQL_TYPE_LONGLONG
        9: sa_mysql.MEDIUMINT,  # MYSQL_TYPE_INT24
        10: sa_mysql.DATE,  # MYSQL_TYPE_DATE
        11: sa_mysql.TIME,  # MYSQL_TYPE_TIME
        12: sa_mysql.DATETIME,  # MYSQL_TYPE_DATETIME
        13: sa_mysql.YEAR,  # MYSQL_TYPE_YEAR
        14: sa_mysql.DATE,  # MYSQL_TYPE_NEWDATE
        15: sa_mysql.VARCHAR,  # MYSQL_TYPE_VARCHAR
        16: sa_mysql.BIT,  # MYSQL_TYPE_BIT
        17: sa_mysql.TIMESTAMP,  # MYSQL_TYPE_TIMESTAMP2
        18: sa_mysql.DATETIME,  # MYSQL_TYPE_DATETIME2
        19: sa_mysql.TIME,  # MYSQL_TYPE_TIME2
        245: sa_mysql.JSON,  # MYSQL_TYPE_JSON
        246: sa_mysql.DECIMAL,  # MYSQL_TYPE_NEWDECIMAL
        247: sa_mysql.ENUM,  # MYSQL_TYPE_ENUM
        248: sa_mysql.SET,  # MYSQL_TYPE_SET
        249: sa_mysql.TINYTEXT,  # MYSQL_TYPE_TINY_BLOB
        250: sa_mysql.MEDIUMTEXT,  # MYSQL_TYPE_MEDIUM_BLOB
        251: sa_mysql.LONGTEXT,  # MYSQL_TYPE_LONG_BLOB
        252: sa_mysql.TEXT,  # MYSQL_TYPE_BLOB
        253: sa_mysql.TEXT,  # MYSQL_TYPE_VAR_STRING
        254: sa_mysql.CHAR,  # MYSQL_TYPE_STRING
    }
