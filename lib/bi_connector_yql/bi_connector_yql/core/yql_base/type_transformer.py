from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Dict, Tuple

import sqlalchemy as sa

import ydb.sqlalchemy as ydb_sa

from dl_constants.enums import BIType, ConnectionType

from dl_core.db.conversion_base import TypeTransformer, make_native_type

if TYPE_CHECKING:
    from dl_core.db.native_type import SATypeSpec


class YQLTypeTransformerBase(TypeTransformer):
    conn_type: ClassVar[ConnectionType]

    _base_type_map: Dict[BIType, Tuple[SATypeSpec, ...]] = {
        # Note: first SA type is used as the default.
        BIType.integer: (
            sa.BIGINT, sa.SMALLINT, sa.INTEGER,
            ydb_sa.types.UInt32, ydb_sa.types.UInt64, ydb_sa.types.UInt8,
        ),
        BIType.float: (
            sa.FLOAT, sa.REAL, sa.NUMERIC,
            # see also: DOUBLE_PRECISION,
        ),
        BIType.boolean: (
            sa.BOOLEAN,
        ),
        BIType.string: (
            sa.TEXT, sa.CHAR, sa.VARCHAR,
            # see also: ENUM,
        ),
        # see also: UUID
        BIType.date: (
            sa.DATE,
        ),
        BIType.datetime: (
            sa.DATETIME, sa.TIMESTAMP,
        ),
        BIType.genericdatetime: (
            sa.DATETIME, sa.TIMESTAMP,
        ),
        BIType.unsupported: (  # Actually the default, so should not matter much.
            sa.sql.sqltypes.NullType,
        ),
    }
    _extra_type_map: Dict[BIType, SATypeSpec] = {  # user-to-native only
        BIType.geopoint: sa.TEXT,
        BIType.geopolygon: sa.TEXT,
        BIType.uuid: sa.TEXT,  # see also: UUID
        BIType.markup: sa.TEXT,
    }

    native_to_user_map = {
        make_native_type(ConnectionType.unknown, sa_type): bi_type
        for bi_type, sa_types in _base_type_map.items()
        for sa_type in sa_types
        if bi_type != BIType.datetime
    }
    user_to_native_map = {
        **{
            bi_type: make_native_type(ConnectionType.unknown, sa_types[0])
            for bi_type, sa_types in _base_type_map.items()
        },
        **{
            bi_type: make_native_type(ConnectionType.unknown, sa_type)
            for bi_type, sa_type in _extra_type_map.items()
        },
    }
