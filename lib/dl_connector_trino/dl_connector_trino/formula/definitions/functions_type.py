import sqlalchemy as sa
from sqlalchemy.sql import functions
import trino.sqlalchemy.datatype as tsa

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    Function,
    SingleVariantTranslationBase,
    TranslationVariant,
)
import dl_formula.definitions.functions_type as base
from dl_formula.definitions.scope import Scope
from dl_formula.translation.context import TranslationCtx

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


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
    # base.FuncDate1FromDatetime.for_dialect(D.TRINO),
    # base.FuncDate1FromString.for_dialect(D.TRINO),
    # base.FuncDate1FromNumber(
    #     variants=[
    #         V(D.TRINO, lambda expr: sa.cast(sa.func.FROM_UNIXTIME(expr), sa.DATE)),
    #     ]
    # ),
    # datetime
    base.FuncDatetime1FromNull.for_dialect(D.TRINO),
    # base.FuncDatetime1FromDatetime.for_dialect(D.TRINO),
    # base.FuncDatetime1FromDate.for_dialect(D.TRINO),
    # base.FuncDatetime1FromNumber(
    #     variants=[
    #         V(D.TRINO, lambda expr: sa.func.FROM_UNIXTIME(expr)),
    #     ]
    # ),
    # base.FuncDatetime1FromString.for_dialect(D.TRINO),
    FuncDatetime2Trino(),
    # datetimetz
    # base.FuncDatetimeTZConst.for_dialect(D.TRINO),
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
    # base.FuncGenericDatetime1FromDatetime.for_dialect(D.TRINO),
    # base.FuncGenericDatetime1FromDate.for_dialect(D.TRINO),
    # base.FuncGenericDatetime1FromNumber(
    #     variants=[
    #         V(D.TRINO, lambda expr: sa.func.FROM_UNIXTIME(expr)),
    #     ]
    # ),
    # base.FuncGenericDatetime1FromString(
    #     variants = [
    #         V(D.TRINO, lambda expr: sa.cast(expr, tsa.TIMESTAMP)),
    #     ]
    # ),
    FuncGenericDatetime2Trino(),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.TRINO),
    # base.FuncGeopointFromCoords.for_dialect(D.TRINO),
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
    base.FuncStrFromString.for_dialect(D.TRINO),
]
