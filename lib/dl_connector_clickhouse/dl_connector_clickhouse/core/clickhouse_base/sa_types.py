from __future__ import annotations

from functools import partial
from typing import (
    Callable,
    Collection,
    Type,
)

from clickhouse_sqlalchemy import types as ch_types
import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_constants.enums import SourceBackendType
from dl_type_transformer.native_type import (
    ClickHouseDateTime64NativeType,
    ClickHouseDateTime64WithTZNativeType,
    ClickHouseDateTimeWithTZNativeType,
    ClickHouseNativeType,
    CommonNativeType,
    GenericNativeType,
)
from dl_type_transformer.type_transformer import make_native_type

from dl_connector_clickhouse.core.clickhouse_base.constants import BACKEND_TYPE_CLICKHOUSE
from dl_connector_clickhouse.core.clickhouse_base.type_transformer import (
    CH_TYPES_DATE,
    CH_TYPES_FLOAT,
    CH_TYPES_INT,
)


def _make_ch_type(nt: GenericNativeType, typeobj: TypeEngine) -> TypeEngine:
    nullable = True
    lowcardinality = False
    if isinstance(nt, CommonNativeType):
        nullable = nt.nullable
    if isinstance(nt, ClickHouseNativeType):
        lowcardinality = nt.lowcardinality
    result = typeobj
    if nullable:
        result = ch_types.Nullable(result)
    if lowcardinality:
        result = ch_types.LowCardinality(result)
    return result


def ch_instantiator(typecls: Type[TypeEngine]) -> Callable:
    def type_gen(nt: GenericNativeType) -> TypeEngine:
        return _make_ch_type(nt=nt, typeobj=typecls())

    return type_gen


def ch_fallback_type_gen(*args, **kwargs):  # type: ignore  # TODO: fix
    return ch_types.Nullable(ch_types.String())


def _make_ch_dtwtz(nt: GenericNativeType, typecls: Type[TypeEngine] = ch_types.DateTimeWithTZ) -> TypeEngine:
    if isinstance(nt, ClickHouseDateTimeWithTZNativeType):
        tz = nt.timezone_name
    else:
        tz = "UTC"
    typeobj = typecls(tz)  # type: ignore  # 2024-01-24 # TODO: Too many arguments for "TypeEngine"  [call-arg]
    return _make_ch_type(nt=nt, typeobj=typeobj)


DEFAULT_DT64_PRECISION = 9


def _make_ch_dt64(nt: GenericNativeType, typecls: Type[TypeEngine] = ch_types.DateTime64) -> TypeEngine:
    if isinstance(nt, ClickHouseDateTime64NativeType):
        precision = nt.precision
    else:
        precision = DEFAULT_DT64_PRECISION
    typeobj = typecls(precision)  # type: ignore  # 2024-01-24 # TODO: Too many arguments for "TypeEngine"  [call-arg]
    return _make_ch_type(nt=nt, typeobj=typeobj)


def _make_ch_dt64wtz(nt: GenericNativeType, typecls: Type[TypeEngine] = ch_types.DateTime64WithTZ) -> TypeEngine:
    if isinstance(nt, ClickHouseDateTime64WithTZNativeType):
        tz = nt.timezone_name
        precision = nt.precision
    else:
        tz = "UTC"
        precision = DEFAULT_DT64_PRECISION
    typeobj = typecls(precision, tz)  # type: ignore  # 2024-01-24 # TODO: Too many arguments for "TypeEngine"  [call-arg]
    return _make_ch_type(nt=nt, typeobj=typeobj)


def _make_ch_array(nt: GenericNativeType, inner_typecls: Type[TypeEngine]) -> TypeEngine:
    ch_type_inner = _make_ch_type(nt=nt, typeobj=inner_typecls())
    return ch_types.Array(ch_type_inner)


SQLALCHEMY_CLICKHOUSE_BASE_TYPES = (
    *CH_TYPES_INT,
    *CH_TYPES_FLOAT,
    ch_types.String,  # TODO: FixedString
    # # TODO: corresponding ClickHouseNativeType subclasses.
    # ch_types.Enum8, ch_types.Enum16, ch_types.Decimal,
    *CH_TYPES_DATE,
    ch_types.DateTime,
    ch_types.UUID,
    ch_types.Bool,
)


def _generate_complex_ch_types(
    base_ch_types: Collection[Type[TypeEngine]],
) -> dict[tuple[SourceBackendType, GenericNativeType], Callable[..., TypeEngine]]:
    return {
        **{(BACKEND_TYPE_CLICKHOUSE, make_native_type(typecls)): ch_instantiator(typecls) for typecls in base_ch_types},
        (BACKEND_TYPE_CLICKHOUSE, make_native_type(ch_types.DateTimeWithTZ)): _make_ch_dtwtz,
        (BACKEND_TYPE_CLICKHOUSE, make_native_type(ch_types.DateTime64)): _make_ch_dt64,
        (BACKEND_TYPE_CLICKHOUSE, make_native_type(ch_types.DateTime64WithTZ)): _make_ch_dt64wtz,
        **{
            (BACKEND_TYPE_CLICKHOUSE, make_native_type(ch_types.Array(typecls))): partial(
                _make_ch_array, inner_typecls=typecls
            )
            for typecls in CH_TYPES_INT
        },
        **{
            (BACKEND_TYPE_CLICKHOUSE, make_native_type(ch_types.Array(typecls))): partial(
                _make_ch_array, inner_typecls=typecls
            )
            for typecls in CH_TYPES_FLOAT
        },
        (BACKEND_TYPE_CLICKHOUSE, make_native_type(ch_types.Array(ch_types.String))): partial(
            _make_ch_array, inner_typecls=ch_types.String
        ),
        # For the `UserDataType.unsupported`; should only be filled with `NULL`s in materialization.
        # See also: `dl_core.data_source.sql.BaseSQLDataSource._make_raw_column_select`
        (BACKEND_TYPE_CLICKHOUSE, make_native_type(sa.sql.sqltypes.NullType)): ch_fallback_type_gen,
    }


SQLALCHEMY_CLICKHOUSE_TYPES = _generate_complex_ch_types(SQLALCHEMY_CLICKHOUSE_BASE_TYPES)
