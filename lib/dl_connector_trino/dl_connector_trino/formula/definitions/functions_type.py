import sqlalchemy as sa
from sqlalchemy.sql import functions
from sqlalchemy.sql.elements import ClauseElement
import trino.sqlalchemy.datatype as tsa

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    Function,
    SingleVariantTranslationBase,
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.common_datetime import ensure_naive_first_arg
import dl_formula.definitions.functions_type as base
from dl_formula.definitions.scope import Scope
from dl_formula.shortcuts import n
from dl_formula.translation.context import TranslationCtx

from dl_connector_trino.formula.constants import TrinoDialect as D
from dl_connector_trino.formula.definitions.functions_array import format_float


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class FuncDbCastTrino(base.FuncDbCastBase):
    WHITELISTS = {
        D.TRINO: {
            DataType.BOOLEAN: [
                base.WhitelistTypeSpec(name="boolean", sa_type=sa.BOOLEAN),
            ],
            DataType.INTEGER: [
                base.WhitelistTypeSpec(name="tinyint", sa_type=sa.SMALLINT),
                base.WhitelistTypeSpec(name="smallint", sa_type=sa.SMALLINT),
                base.WhitelistTypeSpec(name="integer", sa_type=sa.INTEGER),
                base.WhitelistTypeSpec(name="bigint", sa_type=sa.BIGINT),
            ],
            DataType.FLOAT: [
                base.WhitelistTypeSpec(name="real", sa_type=sa.REAL),
                base.WhitelistTypeSpec(name="double", sa_type=tsa.DOUBLE),
                base.WhitelistTypeSpec(name="decimal", sa_type=sa.DECIMAL, arg_types=base.DECIMAL_CAST_ARG_T),
            ],
            DataType.STRING: [
                base.WhitelistTypeSpec(name="varchar", sa_type=sa.VARCHAR),
                base.WhitelistTypeSpec(name="char", sa_type=sa.CHAR),
                base.WhitelistTypeSpec(name="varbinary", sa_type=sa.VARBINARY),
                base.WhitelistTypeSpec(name="json", sa_type=tsa.JSON),
            ],
            DataType.DATE: [
                base.WhitelistTypeSpec(name="date", sa_type=sa.DATE),
            ],
            DataType.DATETIME: [
                base.WhitelistTypeSpec(name="timestamp", sa_type=tsa.TIMESTAMP),
                base.WhitelistTypeSpec(name="timestamp with time zone", sa_type=tsa.TIMESTAMP),
            ],
            DataType.GENERICDATETIME: [
                base.WhitelistTypeSpec(name="timestamp", sa_type=tsa.TIMESTAMP),
                base.WhitelistTypeSpec(name="timestamp with time zone", sa_type=tsa.TIMESTAMP),
            ],
            DataType.ARRAY_STR: [
                base.WhitelistTypeSpec(name="array(varchar)", sa_type=sa.ARRAY, nested_sa_type=sa.VARCHAR),
                base.WhitelistTypeSpec(name="array(char)", sa_type=sa.ARRAY, nested_sa_type=sa.CHAR),
            ],
            DataType.ARRAY_INT: [
                base.WhitelistTypeSpec(name="array(tinyint)", sa_type=sa.ARRAY, nested_sa_type=sa.SMALLINT),
                base.WhitelistTypeSpec(name="array(smallint)", sa_type=sa.ARRAY, nested_sa_type=sa.SMALLINT),
                base.WhitelistTypeSpec(name="array(integer)", sa_type=sa.ARRAY, nested_sa_type=sa.INTEGER),
                base.WhitelistTypeSpec(name="array(bigint)", sa_type=sa.ARRAY, nested_sa_type=sa.BIGINT),
            ],
            DataType.ARRAY_FLOAT: [
                base.WhitelistTypeSpec(name="array(real)", sa_type=sa.ARRAY, nested_sa_type=sa.REAL),
                base.WhitelistTypeSpec(name="array(double)", sa_type=sa.ARRAY, nested_sa_type=tsa.DOUBLE),
                base.WhitelistTypeSpec(
                    name="array(decimal)",
                    sa_type=sa.ARRAY,
                    nested_sa_type=sa.DECIMAL,
                    arg_types=base.DECIMAL_CAST_ARG_T,
                ),
            ],
        }
    }


class FuncDbCastTrino2(FuncDbCastTrino, base.FuncDbCast2):
    pass


class FuncDbCastTrino3(FuncDbCastTrino, base.FuncDbCast3):
    pass


class FuncDbCastTrino4(FuncDbCastTrino, base.FuncDbCast4):
    pass


class FuncStrFromArrayTrino(base.FuncStrFromArray):
    variants = [
        V(D.TRINO, lambda value: "[" + sa.func.array_join(value, ",", "NULL") + "]"),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_STR]),
    ]


class FuncStrFromArrayFloatTrino(base.FuncStrFromArray):
    variants = [
        V(D.TRINO, lambda value: "[" + sa.func.array_join(format_float(value), ",", "NULL") + "]"),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
    ]


class FuncDatetimeTZTrino(SingleVariantTranslationBase, base.FuncDatetimeTZ):
    dialects = D.TRINO
    argument_types = [
        ArgTypeSequence(
            [
                {
                    DataType.DATETIME,
                    DataType.GENERICDATETIME,
                    DataType.DATETIMETZ,
                    DataType.INTEGER,
                    DataType.FLOAT,
                    DataType.STRING,
                },
                DataType.CONST_STRING,
            ]
        ),
    ]

    @classmethod
    def _translate_main(cls, value_ctx: TranslationCtx, tz_ctx: TranslationCtx) -> ClauseElement:
        value = value_ctx.expression
        value_type = value_ctx.data_type
        tz = tz_ctx.expression

        if value_type in (DataType.DATETIMETZ, DataType.GENERICDATETIME):
            assert value
            return value

        elif value_type is DataType.DATETIME:
            dt = value

        elif value_type is DataType.STRING:
            dt = sa.cast(sa.func.from_iso8601_timestamp(value), tsa.TIMESTAMP(timezone=False))

        elif value_type in (DataType.INTEGER, DataType.FLOAT):
            dt = sa.cast(sa.func.from_unixtime(value, "UTC"), tsa.TIMESTAMP(timezone=False))

        else:
            raise Exception("Unexpected value type, likely a translation definition error", value_type)

        dt_tz = sa.func.with_timezone(dt, tz)
        return sa.func.at_timezone(dt_tz, "UTC")


class FuncDatetimeTZToNaiveTrino(base.FuncDatetimeTZToNaive):
    dialects = D.TRINO

    @classmethod
    def _translate_main(cls, value_ctx: TranslationCtx) -> ClauseElement:
        expr = value_ctx.expression
        assert value_ctx.data_type_params
        tz = value_ctx.data_type_params.timezone
        assert tz

        tz_unaware = sa.cast(sa.func.at_timezone(expr, tz), tsa.TIMESTAMP(timezone=False))
        return sa.func.with_timezone(tz_unaware, "UTC")


# Note: `SingleVariantTranslationBase` here essentially acts as a mixin, providing
# custom `get_variants` implementation that plugs into `cls.dialects` and `cls._translate_main`.
class FuncTypeGenericDatetime2TrinoImpl(SingleVariantTranslationBase, base.FuncTypeGenericDatetime2Impl):
    dialects = D.TRINO
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.DATETIME, DataType.GENERICDATETIME, DataType.INTEGER, DataType.FLOAT, DataType.STRING},
                DataType.CONST_STRING,
            ]
        ),
    ]

    @classmethod
    def _translate_main(cls, expr: TranslationCtx, tz: TranslationCtx) -> functions.Function:
        return sa.func.with_timezone(sa.cast(expr.expression, tsa.TIMESTAMP), tz.expression)


class FuncDatetime2Trino(FuncTypeGenericDatetime2TrinoImpl):
    name = "datetime"


class FuncGenericDatetime2Trino(FuncTypeGenericDatetime2TrinoImpl):
    name = "genericdatetime"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


DEFINITIONS_TYPE = [
    # bool
    base.FuncBoolFromNull.for_dialect(D.TRINO),
    base.FuncBoolFromNumber.for_dialect(D.TRINO),
    base.FuncBoolFromBool.for_dialect(D.TRINO),
    base.FuncBoolFromStrGeo.for_dialect(D.TRINO),
    base.FuncBoolFromDateDatetime.for_dialect(D.TRINO),
    # date
    base.FuncDate1FromNull.for_dialect(D.TRINO),
    base.FuncDate1FromDatetime.for_dialect(D.TRINO),
    base.FuncDate1FromDatetimeTZ(
        variants=[
            V(D.TRINO, lambda expr: sa.cast(expr.expression, sa.DATE)),
        ]
    ),
    base.FuncDate1FromString.for_dialect(D.TRINO),
    base.FuncDate1FromNumber(
        variants=[
            V(D.TRINO, lambda value: sa.cast(sa.func.from_unixtime(value), sa.DATE)),
        ]
    ),
    # datetime
    base.FuncDatetime1FromNull.for_dialect(D.TRINO),
    base.FuncDatetime1FromDatetime.for_dialect(D.TRINO),
    base.FuncDatetime1FromDate.for_dialect(D.TRINO),
    base.FuncDatetime1FromNumber(
        variants=[
            V(D.TRINO, sa.func.from_unixtime),
        ]
    ),
    base.FuncDatetime1FromString.for_dialect(D.TRINO),
    FuncDatetime2Trino(),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.TRINO),
    FuncDatetimeTZTrino(),
    # datetimetz_to_naive
    FuncDatetimeTZToNaiveTrino(),
    # db_cast
    FuncDbCastTrino2(),
    FuncDbCastTrino3(),
    FuncDbCastTrino4(),
    # float
    base.FuncFloatNumber(
        variants=[
            V(D.TRINO, lambda value: sa.cast(value, tsa.DOUBLE)),
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.TRINO, lambda value: sa.cast(value, tsa.DOUBLE)),
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(D.TRINO, lambda value: sa.cast(value, tsa.DOUBLE)),
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(D.TRINO, sa.func.to_unixtime),
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.TRINO, sa.func.to_unixtime),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.TRINO, sa.func.to_unixtime),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull.for_dialect(D.TRINO),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.TRINO),
    base.FuncGenericDatetime1FromDate.for_dialect(D.TRINO),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(D.TRINO, sa.func.from_unixtime),
        ]
    ),
    base.FuncGenericDatetime1FromString(
        variants=[
            V(D.TRINO, lambda expr: sa.cast(expr, tsa.TIMESTAMP)),
        ]
    ),
    FuncGenericDatetime2Trino(),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.TRINO),
    base.FuncGeopointFromCoords.for_dialect(D.TRINO),
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.TRINO),
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.TRINO, lambda _: sa.cast(sa.null(), sa.BIGINT)),
        ]
    ),
    # base.FuncIntFromInt.for_dialect(D.TRINO),
    base.FuncIntFromFloat(
        variants=[
            V(D.TRINO, lambda value: sa.cast(sa.func.floor(value), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            V(D.TRINO, lambda value: sa.cast(value, sa.SMALLINT)),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.TRINO, lambda value: sa.cast(value, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(D.TRINO, lambda value: sa.cast(sa.func.floor(sa.func.to_unixtime(value)), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(D.TRINO, lambda value: sa.cast(sa.func.floor(sa.func.to_unixtime(value)), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(D.TRINO, lambda value: sa.cast(sa.func.floor(sa.func.to_unixtime(value)), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDatetimeTZ(
        variants=[
            V(D.TRINO, lambda value: sa.cast(sa.func.floor(sa.func.to_unixtime(value)), sa.BIGINT)),
        ]
    ),
    # str
    # base.FuncStrFromUnsupported(
    #     variants=[
    #         V(D.TRINO, lambda value: sa.cast(value, sa.VARCHAR)),
    #     ]
    # ),
    base.FuncStrFromInteger(
        variants=[
            V(D.TRINO, lambda value: sa.cast(value, sa.VARCHAR)),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            V(D.TRINO, lambda value: sa.func.regexp_extract(sa.func.format("%.20f", value), r"^(-?\d+\.\d+?)(0*)$", 1)),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(D.TRINO, lambda value: sa.case(whens=[(value.is_(None), sa.null()), (value, "True")], else_="False")),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.TRINO),
    base.FuncStrFromDate(
        variants=[
            V(D.TRINO, lambda value: sa.cast(value, sa.VARCHAR)),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(D.TRINO, lambda value: sa.func.date_format(value, "%Y-%m-%d %H:%i:%s")),
        ]
    ),
    base.FuncStrFromDatetimeTZ(
        variants=[
            VW(D.TRINO, ensure_naive_first_arg(n.func.STR)),
        ]
    ),
    base.FuncStrFromString.for_dialect(D.TRINO),
    FuncStrFromArrayTrino(),
    FuncStrFromArrayFloatTrino(),
]
