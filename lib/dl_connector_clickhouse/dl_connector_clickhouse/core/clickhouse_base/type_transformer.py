from __future__ import annotations

from clickhouse_sqlalchemy import types as ch_types
import sqlalchemy as sa

from dl_constants.enums import UserDataType
from dl_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)


CH_TYPES_INT = frozenset(
    (
        ch_types.Int,
        ch_types.Int8,
        ch_types.Int16,
        ch_types.Int32,
        ch_types.Int64,
        ch_types.UInt8,
        ch_types.UInt16,
        ch_types.UInt32,
        ch_types.UInt64,
    )
)
CH_TYPES_FLOAT = frozenset((ch_types.Float, ch_types.Float32, ch_types.Float64, ch_types.Decimal))
CH_TYPES_DATE = frozenset((ch_types.Date, ch_types.Date32))


class ClickHouseTypeTransformer(TypeTransformer):
    native_to_user_map = {
        **{make_native_type(typecls): UserDataType.integer for typecls in CH_TYPES_INT},  # type: ignore  # TODO: fix
        **{make_native_type(typecls): UserDataType.string for typecls in (ch_types.String,)},  # TODO: FixedString
        make_native_type(ch_types.Enum8): UserDataType.string,
        make_native_type(ch_types.Enum16): UserDataType.string,
        **{make_native_type(typecls): UserDataType.float for typecls in CH_TYPES_FLOAT},
        make_native_type(ch_types.Date): UserDataType.date,
        make_native_type(ch_types.Date32): UserDataType.date,
        make_native_type(ch_types.Bool): UserDataType.boolean,
        make_native_type(ch_types.DateTime): UserDataType.genericdatetime,
        make_native_type(ch_types.DateTime64): UserDataType.genericdatetime,
        make_native_type(ch_types.DateTimeWithTZ): UserDataType.genericdatetime,
        make_native_type(ch_types.DateTime64WithTZ): UserDataType.genericdatetime,
        make_native_type(ch_types.UUID): UserDataType.uuid,
        **{make_native_type(ch_types.Array(typecls)): UserDataType.array_int for typecls in CH_TYPES_INT},
        **{make_native_type(ch_types.Array(typecls)): UserDataType.array_float for typecls in CH_TYPES_FLOAT},
        make_native_type(ch_types.Array(ch_types.String())): UserDataType.array_str,
        make_native_type(sa.sql.sqltypes.NullType): UserDataType.unsupported,
    }
    user_to_native_map = {
        UserDataType.integer: make_native_type(ch_types.Int64),
        UserDataType.float: make_native_type(ch_types.Float64),
        UserDataType.boolean: make_native_type(ch_types.Bool),
        UserDataType.string: make_native_type(ch_types.String),
        UserDataType.date: make_native_type(ch_types.Date),
        UserDataType.datetime: make_native_type(ch_types.DateTime),  # TODO: DateTime64
        UserDataType.genericdatetime: make_native_type(ch_types.DateTime),  # TODO: DateTime64
        # WARNING: underparametrized
        UserDataType.datetimetz: make_native_type(ch_types.DateTimeWithTZ),  # TODO: DateTime64WithTZ
        UserDataType.geopoint: make_native_type(ch_types.String),
        UserDataType.geopolygon: make_native_type(ch_types.String),
        UserDataType.uuid: make_native_type(ch_types.UUID),
        UserDataType.markup: make_native_type(ch_types.String),
        UserDataType.array_int: make_native_type(ch_types.Array(ch_types.Int64)),
        UserDataType.array_float: make_native_type(ch_types.Array(ch_types.Float64)),
        UserDataType.array_str: make_native_type(ch_types.Array(ch_types.String)),
        UserDataType.tree_str: make_native_type(ch_types.Array(ch_types.String)),
        UserDataType.unsupported: make_native_type(sa.sql.sqltypes.NullType),
    }
