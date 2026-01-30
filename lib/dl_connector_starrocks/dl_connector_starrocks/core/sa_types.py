from __future__ import annotations

from sqlalchemy.dialects import mysql as mysql_types

from dl_type_transformer.sa_types_base import simple_instantiator
from dl_type_transformer.type_transformer import make_native_type

from dl_connector_starrocks.core.constants import BACKEND_TYPE_STARROCKS
from dl_connector_starrocks.core.type_transformer import (
    STARROCKS_TYPES_FLOAT,
    STARROCKS_TYPES_INT,
    STARROCKS_TYPES_STRING,
)


SQLALCHEMY_STARROCKS_BASE_TYPES = (
    *STARROCKS_TYPES_INT,
    *STARROCKS_TYPES_FLOAT,
    *STARROCKS_TYPES_STRING,
    mysql_types.BOOLEAN,
    mysql_types.DATE,
    mysql_types.DATETIME,
    mysql_types.TIMESTAMP,
)

SQLALCHEMY_STARROCKS_TYPES = {
    **{
        (BACKEND_TYPE_STARROCKS, make_native_type(typecls)): simple_instantiator(typecls)
        for typecls in SQLALCHEMY_STARROCKS_BASE_TYPES
    },
}
