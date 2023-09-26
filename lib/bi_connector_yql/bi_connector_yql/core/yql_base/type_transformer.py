from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Tuple,
)

import sqlalchemy as sa
import ydb.sqlalchemy as ydb_sa

from dl_constants.enums import (
    ConnectionType,
    UserDataType,
)
from dl_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)


if TYPE_CHECKING:
    from dl_core.db.native_type import SATypeSpec


class YQLTypeTransformerBase(TypeTransformer):
    conn_type: ClassVar[ConnectionType]

    _base_type_map: Dict[UserDataType, Tuple[SATypeSpec, ...]] = {
        # Note: first SA type is used as the default.
        UserDataType.integer: (
            sa.BIGINT,
            sa.SMALLINT,
            sa.INTEGER,
            ydb_sa.types.UInt32,
            ydb_sa.types.UInt64,
            ydb_sa.types.UInt8,
        ),
        UserDataType.float: (
            sa.FLOAT,
            sa.REAL,
            sa.NUMERIC,
            # see also: DOUBLE_PRECISION,
        ),
        UserDataType.boolean: (sa.BOOLEAN,),
        UserDataType.string: (
            sa.TEXT,
            sa.CHAR,
            sa.VARCHAR,
            # see also: ENUM,
        ),
        # see also: UUID
        UserDataType.date: (sa.DATE,),
        UserDataType.datetime: (
            sa.DATETIME,
            sa.TIMESTAMP,
        ),
        UserDataType.genericdatetime: (
            sa.DATETIME,
            sa.TIMESTAMP,
        ),
        UserDataType.unsupported: (sa.sql.sqltypes.NullType,),  # Actually the default, so should not matter much.
    }
    _extra_type_map: Dict[UserDataType, SATypeSpec] = {  # user-to-native only
        UserDataType.geopoint: sa.TEXT,
        UserDataType.geopolygon: sa.TEXT,
        UserDataType.uuid: sa.TEXT,  # see also: UUID
        UserDataType.markup: sa.TEXT,
    }

    native_to_user_map = {
        make_native_type(ConnectionType.unknown, sa_type): bi_type
        for bi_type, sa_types in _base_type_map.items()
        for sa_type in sa_types
        if bi_type != UserDataType.datetime
    }
    user_to_native_map = {
        **{
            bi_type: make_native_type(ConnectionType.unknown, sa_types[0])
            for bi_type, sa_types in _base_type_map.items()
        },
        **{bi_type: make_native_type(ConnectionType.unknown, sa_type) for bi_type, sa_type in _extra_type_map.items()},
    }
