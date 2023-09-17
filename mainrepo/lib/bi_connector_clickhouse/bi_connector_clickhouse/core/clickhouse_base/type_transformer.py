from __future__ import annotations

from clickhouse_sqlalchemy import types as ch_types
import sqlalchemy as sa

from bi_constants.enums import BIType
from bi_core.backend_types import get_backend_type
from bi_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)
from bi_core.db.elements import GenericNativeType

from bi_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE

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
    conn_type = CONNECTION_TYPE_CLICKHOUSE
    native_to_user_map = {
        **{make_native_type(CONNECTION_TYPE_CLICKHOUSE, typecls): BIType.integer for typecls in CH_TYPES_INT},  # type: ignore  # TODO: fix
        **{
            make_native_type(CONNECTION_TYPE_CLICKHOUSE, typecls): BIType.string
            for typecls in (ch_types.String,)  # TODO: FixedString
        },
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Enum8): BIType.string,
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Enum16): BIType.string,
        **{make_native_type(CONNECTION_TYPE_CLICKHOUSE, typecls): BIType.float for typecls in CH_TYPES_FLOAT},
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Date): BIType.date,
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Date32): BIType.date,
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Bool): BIType.boolean,
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.DateTime): BIType.genericdatetime,
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.DateTime64): BIType.genericdatetime,
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.DateTimeWithTZ): BIType.genericdatetime,
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.DateTime64WithTZ): BIType.genericdatetime,
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.UUID): BIType.uuid,
        **{
            make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Array(typecls)): BIType.array_int
            for typecls in CH_TYPES_INT
        },
        **{
            make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Array(typecls)): BIType.array_float
            for typecls in CH_TYPES_FLOAT
        },
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Array(ch_types.String())): BIType.array_str,
        make_native_type(CONNECTION_TYPE_CLICKHOUSE, sa.sql.sqltypes.NullType): BIType.unsupported,
    }
    user_to_native_map = {
        BIType.integer: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Int64),
        BIType.float: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Float64),
        BIType.boolean: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Bool),
        BIType.string: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.String),
        BIType.date: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Date),
        BIType.datetime: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.DateTime),  # TODO: DateTime64
        BIType.genericdatetime: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.DateTime),  # TODO: DateTime64
        # WARNING: underparametrized
        BIType.datetimetz: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.DateTimeWithTZ),  # TODO: DateTime64WithTZ
        BIType.geopoint: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.String),
        BIType.geopolygon: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.String),
        BIType.uuid: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.UUID),
        BIType.markup: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.String),
        BIType.array_int: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Array(ch_types.Int64)),
        BIType.array_float: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Array(ch_types.Float64)),
        BIType.array_str: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Array(ch_types.String)),
        BIType.tree_str: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Array(ch_types.String)),
        BIType.unsupported: make_native_type(CONNECTION_TYPE_CLICKHOUSE, sa.sql.sqltypes.NullType),
    }

    def make_foreign_native_type_conversion(self, native_t: GenericNativeType) -> GenericNativeType:
        # All CH conn_types are supposed to have equivalent types.
        nt_backend_type = get_backend_type(conn_type=native_t.conn_type)
        own_backend_type = get_backend_type(self.conn_type)

        if nt_backend_type == own_backend_type:
            return native_t.clone(conn_type=self.conn_type)
        return super().make_foreign_native_type_conversion(native_t)
