from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
import ydb_sqlalchemy.sqlalchemy as ydb_sa

from dl_constants.enums import UserDataType
import dl_sqlalchemy_ydb.dialect
import dl_sqlalchemy_ydb.dialect as ydb_dialect
from dl_type_transformer.type_transformer import (
    TypeTransformer,
    make_native_type,
)


if TYPE_CHECKING:
    from dl_type_transformer.native_type import SATypeSpec


class YQLTypeTransformer(TypeTransformer):
    _base_type_map: dict[UserDataType, tuple[SATypeSpec, ...]] = {
        # Note: first SA type is used as the default.
        UserDataType.integer: (
            sa.INTEGER,
            ydb_sa.types.Int8,
            ydb_sa.types.Int16,
            ydb_sa.types.Int32,
            ydb_sa.types.Int64,
            ydb_sa.types.UInt8,
            ydb_sa.types.UInt16,
            ydb_sa.types.UInt32,
            ydb_sa.types.UInt64,
            dl_sqlalchemy_ydb.dialect.YqlInterval,
        ),
        UserDataType.float: (
            sa.FLOAT,
            sa.REAL,
            sa.NUMERIC,
            ydb_dialect.YqlDouble,
            ydb_dialect.YqlFloat,
            # see also: DOUBLE_PRECISION,
        ),
        UserDataType.boolean: (sa.BOOLEAN,),
        UserDataType.string: (
            sa.TEXT,
            ydb_dialect.YqlString,
            ydb_dialect.YqlUtf8,
            sa.String,
            sa.CHAR,
            sa.VARCHAR,
            sa.BINARY,
            # TODO: ydb_sa.types.YqlJSON,
            # see also: ENUM,
        ),
        # see also: UUID
        UserDataType.date: (dl_sqlalchemy_ydb.dialect.YqlDate,),
        UserDataType.datetime: (
            sa.DATETIME,
            sa.TIMESTAMP,
            dl_sqlalchemy_ydb.dialect.YqlDateTime,
            dl_sqlalchemy_ydb.dialect.YqlTimestamp,
        ),
        UserDataType.genericdatetime: (
            sa.DATETIME,
            sa.TIMESTAMP,
            dl_sqlalchemy_ydb.dialect.YqlDateTime,
            dl_sqlalchemy_ydb.dialect.YqlTimestamp,
        ),
        UserDataType.unsupported: (sa.sql.sqltypes.NullType,),  # Actually the default, so should not matter much.
        UserDataType.uuid: (ydb_dialect.YqlUuid,),
    }
    _extra_type_map: dict[UserDataType, SATypeSpec] = {  # user-to-native only
        UserDataType.geopoint: sa.TEXT,
        UserDataType.geopolygon: sa.TEXT,
        UserDataType.markup: sa.TEXT,
    }

    native_to_user_map = {
        make_native_type(sa_type): bi_type
        for bi_type, sa_types in _base_type_map.items()
        for sa_type in sa_types
        if bi_type != UserDataType.datetime
    }
    user_to_native_map = {
        **{bi_type: make_native_type(sa_types[0]) for bi_type, sa_types in _base_type_map.items()},
        **{bi_type: make_native_type(sa_type) for bi_type, sa_type in _extra_type_map.items()},
    }
