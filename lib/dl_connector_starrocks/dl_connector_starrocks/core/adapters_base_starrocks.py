import attr
import sqlalchemy.dialects.mysql as sa_mysql

from dl_type_transformer.native_type import SATypeSpec

from dl_connector_starrocks.core.constants import CONNECTION_TYPE_STARROCKS
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


@attr.s(cmp=False)
class BaseStarRocksAdapter:
    _target_dto: StarRocksConnTargetDTO = attr.ib()

    conn_type = CONNECTION_TYPE_STARROCKS

    # StarRocks uses MySQL wire protocol, so type codes are mostly MySQL-compatible.
    # Integer keys are MySQL protocol field type codes from field_types.h:
    # https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
    # StarRocks types: https://docs.starrocks.io/docs/sql-reference/data-types/
    _type_code_to_sa: dict[int, SATypeSpec] | None = {
        1: sa_mysql.TINYINT,  #   MYSQL_TYPE_TINY        (BOOLEAN, TINYINT)
        2: sa_mysql.SMALLINT,  #  MYSQL_TYPE_SHORT       (SMALLINT)
        3: sa_mysql.INTEGER,  #   MYSQL_TYPE_LONG        (INT)
        4: sa_mysql.FLOAT,  #     MYSQL_TYPE_FLOAT       (FLOAT)
        5: sa_mysql.DOUBLE,  #    MYSQL_TYPE_DOUBLE      (DOUBLE)
        8: sa_mysql.BIGINT,  #    MYSQL_TYPE_LONGLONG    (BIGINT)
        10: sa_mysql.DATE,  #     MYSQL_TYPE_DATE        (DATE)
        11: sa_mysql.TIME,  #     MYSQL_TYPE_TIME        (TIME)
        12: sa_mysql.DATETIME,  # MYSQL_TYPE_DATETIME    (DATETIME)
        246: sa_mysql.DECIMAL,  # MYSQL_TYPE_NEWDECIMAL  (DECIMAL)
        252: sa_mysql.TEXT,  #    MYSQL_TYPE_BLOB        (VARBINARY)
        253: sa_mysql.VARCHAR,  # MYSQL_TYPE_VAR_STRING  (VARCHAR, STRING)
        254: sa_mysql.CHAR,  #    MYSQL_TYPE_STRING      (LARGEINT, CHAR, JSON, HLL, BITMAP, ARRAY, MAP, STRUCT)
    }
