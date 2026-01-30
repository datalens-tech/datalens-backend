import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_type as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


DEFINITIONS_TYPE = [
    # bool
    base.FuncBoolFromNull(
        variants=[
            V(D.STARROCKS, lambda _: sa.cast(sa.null(), sa.BOOLEAN)),
        ]
    ),
    base.FuncBoolFromNumber.for_dialect(D.STARROCKS),
    base.FuncBoolFromBool.for_dialect(D.STARROCKS),
    base.FuncBoolFromStrGeo.for_dialect(D.STARROCKS),
    base.FuncBoolFromDateDatetime.for_dialect(D.STARROCKS),
    # date
    base.FuncDate1FromNull.for_dialect(D.STARROCKS),
    base.FuncDate1FromDatetime.for_dialect(D.STARROCKS),
    base.FuncDate1FromString.for_dialect(D.STARROCKS),
    base.FuncDate1FromNumber(
        variants=[
            V(D.STARROCKS, lambda expr: sa.cast(sa.func.FROM_UNIXTIME(expr), sa.Date())),
        ]
    ),
    # datetime
    base.FuncDatetime1FromNull.for_dialect(D.STARROCKS),
    base.FuncDatetime1FromDatetime.for_dialect(D.STARROCKS),
    base.FuncDatetime1FromDate.for_dialect(D.STARROCKS),
    base.FuncDatetime1FromNumber(
        variants=[
            V(D.STARROCKS, lambda expr: sa.func.FROM_UNIXTIME(expr)),
        ]
    ),
    base.FuncDatetime1FromString.for_dialect(D.STARROCKS),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.STARROCKS),
    # float
    base.FuncFloatNumber(
        variants=[
            V(D.STARROCKS, lambda value: value + sa.literal(0.0)),
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.STARROCKS, lambda value: value + sa.literal(0.0)),
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(D.STARROCKS, lambda value: value + sa.literal(0.0)),
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(sa.func.UNIX_TIMESTAMP(value), sa.DOUBLE)),
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(sa.func.UNIX_TIMESTAMP(value), sa.DOUBLE)),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(sa.func.UNIX_TIMESTAMP(value), sa.DOUBLE)),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull.for_dialect(D.STARROCKS),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.STARROCKS),
    base.FuncGenericDatetime1FromDate.for_dialect(D.STARROCKS),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(D.STARROCKS, lambda expr: sa.func.FROM_UNIXTIME(expr)),
        ]
    ),
    base.FuncGenericDatetime1FromString.for_dialect(D.STARROCKS),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.STARROCKS),
    base.FuncGeopointFromCoords.for_dialect(D.STARROCKS),
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.STARROCKS),
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.STARROCKS, lambda _: sa.cast(sa.null(), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromInt.for_dialect(D.STARROCKS),
    base.FuncIntFromFloat(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(sa.func.FLOOR(value), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            V(D.STARROCKS, lambda value: value),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(value, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(D.STARROCKS, sa.func.UNIX_TIMESTAMP),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(sa.func.UNIX_TIMESTAMP(value), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(sa.func.UNIX_TIMESTAMP(value), sa.BIGINT)),
        ]
    ),
    # str
    base.FuncStrFromUnsupported(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(value, sa.CHAR)),
        ]
    ),
    base.FuncStrFromInteger(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(value, sa.CHAR)),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(value, sa.CHAR)),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(D.STARROCKS, lambda value: sa.case(whens=[(value.is_(None), sa.null()), (value, "True")], else_="False")),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.STARROCKS),
    base.FuncStrFromDate(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(value, sa.CHAR)),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(D.STARROCKS, lambda value: sa.cast(value, sa.CHAR)),
        ]
    ),
    base.FuncStrFromString.for_dialect(D.STARROCKS),
    # tree
    base.FuncTreeStr.for_dialect(D.STARROCKS),
]
