from __future__ import annotations

from typing import Optional

import attr
import sqlalchemy.dialects.mysql as sa_mysql

from dl_type_transformer.native_type import SATypeSpec

from dl_connector_starrocks.core.constants import CONNECTION_TYPE_STARROCKS
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


@attr.s(cmp=False)
class BaseStarRocksAdapter:
    _target_dto: StarRocksConnTargetDTO = attr.ib()

    conn_type = CONNECTION_TYPE_STARROCKS

    # StarRocks uses MySQL wire protocol, so type codes are MySQL-compatible
    _type_code_to_sa: Optional[dict[int, SATypeSpec]] = {
        1: sa_mysql.TINYINT,
        2: sa_mysql.SMALLINT,
        9: sa_mysql.MEDIUMINT,
        3: sa_mysql.INTEGER,
        8: sa_mysql.BIGINT,
        4: sa_mysql.FLOAT,
        5: sa_mysql.DOUBLE,
        0: sa_mysql.DECIMAL,
        246: sa_mysql.DECIMAL,
        7: sa_mysql.TIMESTAMP,
        17: sa_mysql.TIMESTAMP,
        10: sa_mysql.DATE,
        14: sa_mysql.DATE,
        11: sa_mysql.TIME,
        19: sa_mysql.TIME,
        12: sa_mysql.DATETIME,
        18: sa_mysql.DATETIME,
        13: sa_mysql.YEAR,
        16: sa_mysql.BIT,
        247: sa_mysql.ENUM,
        248: sa_mysql.SET,
        15: sa_mysql.VARCHAR,
        249: sa_mysql.TINYTEXT,
        250: sa_mysql.MEDIUMTEXT,
        251: sa_mysql.LONGTEXT,
        252: sa_mysql.TEXT,
        253: sa_mysql.TEXT,
        254: sa_mysql.CHAR,
        245: sa_mysql.JSON,
    }
