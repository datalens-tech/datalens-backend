from typing import Sequence

import clickhouse_sqlalchemy.types as ch_types
import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectCombo
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    Function,
    SingleVariantTranslationBase,
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.common_datetime import (
    ch_date_with_tz,
    ensure_naive_first_arg,
)
import dl_formula.definitions.functions_type as base
from dl_formula.definitions.scope import Scope
from dl_formula.shortcuts import n
from dl_formula.translation.context import TranslationCtx

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D

V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class FuncDate2FromNumber(base.FuncDate2):
    variants = [
        V(D.CLICKHOUSE, lambda expr, tz: sa.func.toDate(expr, tz)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.CONST_STRING]),
        ArgTypeSequence([DataType.INTEGER, DataType.CONST_STRING]),
    ]


class FuncDatetimeTZCH(SingleVariantTranslationBase, base.FuncDatetimeTZ):
    dialects = D.CLICKHOUSE
    argument_types = [
        ArgTypeSequence(
            [
                {
                    DataType.DATETIME,
                    DataType.GENERICDATETIME,
                    DataType.DATETIMETZ,
                    # Not even tried: `DataType.DATE`.
                    DataType.INTEGER,
                    DataType.FLOAT,
                    DataType.STRING,
                },
                DataType.CONST_STRING,
            ]
        ),
    ]

    @classmethod
    def _translate_main(cls, value_ctx, tz_ctx):  # type: ignore
        # NOTE: Tricky point:
        # in CH, converting all aware datetimes to UTC,
        # primarily for the correct output.
        # In particular datetimetz-taking functions such as `DATE()`, *MUST*
        # convert to the `data_type_params.timezone` in CH before applying
        # the CH function.
        # (similarly, in PG, there's only UTC tz-aware datetimes)
        # Reminder: tz-naive datetimes in CH should also be, effectively, in
        # UTC (as there's no proper tz-naive datetimes in CH).
        expr = value_ctx.expression
        if value_ctx.data_type == DataType.DATETIME:
            # Have to pass the naive-datetime values through a string,
            # otherwise CH uses the internal TZ knowledge to shift the value,
            # rather than re-interpret it at the specified timezone.
            expr = sa.func.formatDateTime(expr, "%Y-%m-%d %H:%M:%S")
        expr = sa.func.toDateTime(expr, tz_ctx.expression)  # 'interpret in', effectively
        expr = sa.func.toDateTime(expr, "UTC")
        return expr


class FuncDatetimeTZToNaiveCH(base.FuncDatetimeTZToNaive):
    dialects = D.CLICKHOUSE

    @classmethod
    def _translate_main(cls, value_ctx):  # type: ignore
        expr = value_ctx.expression
        tz = value_ctx.data_type_params.timezone
        assert tz
        # use cases: datepart, grouping (tz transitions should actually group into one value)
        expr = sa.func.toDateTime(expr, tz)
        # TODO: find a better way to tz-shift the value in CH: https://github.com/ClickHouse/ClickHouse/issues/19768
        expr = sa.func.toDateTime(sa.func.formatDateTime(expr, "%Y-%m-%d %H:%M:%S"), "UTC")
        return expr


# Note: `SingleVariantTranslationBase` here essentially acts as a mixin, providing
# custom `get_variants` implementation that plugs into `cls.dialects` and `cls._translate_main`.
class FuncTypeGenericDatetime2CHImpl(SingleVariantTranslationBase, base.FuncTypeGenericDatetime2Impl):
    dialects = D.CLICKHOUSE
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.DATETIME, DataType.GENERICDATETIME, DataType.INTEGER, DataType.FLOAT, DataType.STRING},
                DataType.CONST_STRING,
            ]
        ),
    ]

    @classmethod
    def _translate_main(cls, expr, tz):  # type: ignore
        return sa.func.toDateTime(expr.expression, tz.expression)


class FuncDatetime2CH(FuncTypeGenericDatetime2CHImpl):
    name = "datetime"


class FuncGenericDatetime2CH(FuncTypeGenericDatetime2CHImpl):
    name = "genericdatetime"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncDbCastClickHouseBase(base.FuncDbCastBase):
    WHITELISTS = {
        D.CLICKHOUSE: {
            DataType.INTEGER: [
                base.WhitelistTypeSpec(name="Int8", sa_type=ch_types.Int8),
                base.WhitelistTypeSpec(name="Int16", sa_type=ch_types.Int16),
                base.WhitelistTypeSpec(name="Int32", sa_type=ch_types.Int32),
                base.WhitelistTypeSpec(name="Int64", sa_type=ch_types.Int64),
                base.WhitelistTypeSpec(name="UInt8", sa_type=ch_types.UInt8),
                base.WhitelistTypeSpec(name="UInt16", sa_type=ch_types.UInt16),
                base.WhitelistTypeSpec(name="UInt32", sa_type=ch_types.UInt32),
                base.WhitelistTypeSpec(name="UInt64", sa_type=ch_types.UInt64),
            ],
            DataType.FLOAT: [
                base.WhitelistTypeSpec(name="Float32", sa_type=ch_types.Float32),
                base.WhitelistTypeSpec(name="Float64", sa_type=ch_types.Float64),
                base.WhitelistTypeSpec(name="Decimal", sa_type=ch_types.Decimal, arg_types=base.DECIMAL_CAST_ARG_T),
            ],
            DataType.STRING: [
                base.WhitelistTypeSpec(name="String", sa_type=ch_types.String),
                # TODO: FixedString
            ],
            DataType.DATE: [
                base.WhitelistTypeSpec(name="Date", sa_type=ch_types.Date),
                base.WhitelistTypeSpec(name="Date32", sa_type=ch_types.Date32),
            ],
        }
    }

    @classmethod
    def generate_cast_type(
        cls, dialect: DialectCombo, wr_name: TranslationCtx, value: TranslationCtx, type_args: Sequence[TranslationCtx]
    ) -> TypeEngine:
        # Add Nullable wrapper
        type_ = super().generate_cast_type(dialect=dialect, wr_name=wr_name, value=value, type_args=type_args)
        type_ = ch_types.Nullable(type_)
        return type_


class FuncDbCastClickHouse2(FuncDbCastClickHouseBase, base.FuncDbCast2):
    pass


class FuncDbCastClickHouse3(FuncDbCastClickHouseBase, base.FuncDbCast3):
    pass


class FuncDbCastClickHouse4(FuncDbCastClickHouseBase, base.FuncDbCast4):
    pass


DEFINITIONS_TYPE = [
    # bool
    base.FuncBoolFromNull(
        variants=[
            V(D.CLICKHOUSE, lambda _: sa.func.toInt8(sa.null())),
        ]
    ),
    base.FuncBoolFromNumber.for_dialect(D.CLICKHOUSE),
    base.FuncBoolFromBool.for_dialect(D.CLICKHOUSE),
    base.FuncBoolFromStrGeo.for_dialect(D.CLICKHOUSE),
    base.FuncBoolFromDateDatetime.for_dialect(D.CLICKHOUSE),
    # date
    base.FuncDate1FromNull(
        variants=[
            V(D.CLICKHOUSE, lambda _: sa.func.toDate(sa.null())),
        ]
    ),
    base.FuncDate1FromDatetime(
        variants=[
            VW(D.CLICKHOUSE, lambda value: sa.func.toDate(value.expression)),
        ]
    ),
    base.FuncDate1FromDatetimeTZ(
        variants=[
            VW(D.CLICKHOUSE, lambda value_ctx: sa.func.toDate(*ch_date_with_tz(value_ctx))),
        ]
    ),
    base.FuncDate1FromString(
        variants=[
            V(D.CLICKHOUSE, lambda expr: sa.cast(expr, ch_types.Nullable(ch_types.Date))),
            # V(D.CLICKHOUSE, lambda expr: sa.func.toDateOrNull(expr)),  FIXME: depends on ClickHouse/issues/9584
        ]
    ),
    base.FuncDate1FromNumber(
        variants=[
            V(D.CLICKHOUSE, lambda expr: sa.func.toDate(expr)),
        ]
    ),
    FuncDate2FromNumber(),
    # date_parse
    base.FuncDateParse(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.toDate(sa.func.parseDateTimeBestEffortOrNull(value))),
        ]
    ),
    # datetime
    base.FuncDatetime1FromNull(
        variants=[
            V(D.CLICKHOUSE, lambda _: sa.func.toDateTime(sa.null())),
        ]
    ),
    base.FuncDatetime1FromDatetime.for_dialect(D.CLICKHOUSE),
    base.FuncDatetime1FromDate(
        variants=[
            V(D.CLICKHOUSE, lambda expr: sa.func.toDateTime(expr)),
        ]
    ),
    base.FuncDatetime1FromNumber(
        variants=[
            V(D.CLICKHOUSE, lambda expr: sa.func.toDateTime(expr)),
        ]
    ),
    base.FuncDatetime1FromString(
        variants=[
            V(D.CLICKHOUSE, lambda expr: sa.func.toDateTimeOrNull(expr)),
        ]
    ),
    FuncDatetime2CH(),
    # datetime_parse
    base.FuncDatetimeParse(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.parseDateTimeBestEffortOrNull(value)),
        ]
    ),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.CLICKHOUSE),
    FuncDatetimeTZCH(),
    # datetimetz_to_naive
    FuncDatetimeTZToNaiveCH(),
    # db_cast
    FuncDbCastClickHouse2(),
    FuncDbCastClickHouse3(),
    FuncDbCastClickHouse4(),
    # float
    base.FuncFloatNumber(
        variants=[
            V(D.CLICKHOUSE, sa.func.toFloat64),
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.CLICKHOUSE, sa.func.toFloat64OrNull),
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(D.CLICKHOUSE, sa.func.toFloat64),
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda value: sa.func.toFloat64(sa.func.toUnixTimestamp(sa.func.toDateTime(value, "UTC"), "UTC")),
            ),
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.toFloat64(sa.func.toUnixTimestamp(value, "UTC"))),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.toFloat64(sa.func.toUnixTimestamp(value))),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull(
        variants=[
            V(D.CLICKHOUSE, lambda _: sa.func.toDateTime(sa.null())),
        ]
    ),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.CLICKHOUSE),
    base.FuncGenericDatetime1FromDate(
        variants=[
            V(D.CLICKHOUSE, lambda expr: sa.func.toDateTime(expr)),
        ]
    ),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(D.CLICKHOUSE, lambda expr: sa.func.toDateTime(expr)),
        ]
    ),
    base.FuncGenericDatetime1FromString(
        variants=[
            V(D.CLICKHOUSE, lambda expr: sa.func.toDateTimeOrNull(expr)),
        ]
    ),
    FuncGenericDatetime2CH(),
    # genericdatetime_parse
    base.FuncGenericDatetimeParse(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.parseDateTimeBestEffortOrNull(value)),
        ]
    ),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.CLICKHOUSE),
    base.FuncGeopointFromCoords.for_dialect(D.CLICKHOUSE),
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.CLICKHOUSE),
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.CLICKHOUSE, lambda _: sa.func.toInt64(sa.null())),
        ]
    ),
    base.FuncIntFromInt.for_dialect(D.CLICKHOUSE),
    base.FuncIntFromFloat(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.toInt64(sa.func.floor(value))),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            V(D.CLICKHOUSE, lambda value: value),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.CLICKHOUSE, sa.func.toInt64OrNull),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.toUnixTimestamp(sa.func.toDateTime(value, "UTC"), "UTC")),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.toUnixTimestamp(value, "UTC")),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.toUnixTimestamp(value)),
        ]
    ),
    base.FuncIntFromDatetimeTZ(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.toUnixTimestamp(value, "UTC")),
        ]
    ),
    # str
    base.FuncStrFromUnsupported(
        variants=[
            V(D.CLICKHOUSE, sa.func.toString),
        ]
    ),
    base.FuncStrFromInteger(
        variants=[
            V(D.CLICKHOUSE, sa.func.toString),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            V(D.CLICKHOUSE, sa.func.toString),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(D.CLICKHOUSE, lambda value: getattr(sa.func, "if")(value, "True", "False")),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.CLICKHOUSE),
    base.FuncStrFromDate(
        variants=[
            V(D.CLICKHOUSE, sa.func.toString),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(D.CLICKHOUSE, lambda value: sa.func.toString(value)),
        ]
    ),
    base.FuncStrFromDatetimeTZ(
        variants=[
            VW(D.CLICKHOUSE, ensure_naive_first_arg(n.func.STR)),
        ]
    ),
    base.FuncStrFromString.for_dialect(D.CLICKHOUSE),
    base.FuncStrFromUUID(
        variants=[
            V(D.CLICKHOUSE, sa.func.toString),
        ]
    ),
    base.FuncStrFromArray(
        variants=[
            V(D.CLICKHOUSE, sa.func.toString),
        ]
    ),
    # tree
    base.FuncTreeStr.for_dialect(D.CLICKHOUSE),
]
