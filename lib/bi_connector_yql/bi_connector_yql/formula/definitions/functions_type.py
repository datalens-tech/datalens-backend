import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_type as base

from bi_connector_yql.formula.constants import YqlDialect as D

V = TranslationVariant.make


DEFINITIONS_TYPE = [
    # bool
    base.FuncBoolFromNull.for_dialect(D.YQL),
    base.FuncBoolFromNumber.for_dialect(D.YQL),
    base.FuncBoolFromBool.for_dialect(D.YQL),
    base.FuncBoolFromStrGeo.for_dialect(D.YQL),
    base.FuncBoolFromDateDatetime.for_dialect(D.YQL),
    # date
    base.FuncDate1FromNull.for_dialect(D.YQL),
    base.FuncDate1FromDatetime.for_dialect(D.YQL),
    base.FuncDate1FromString.for_dialect(D.YQL),
    base.FuncDate1FromNumber(
        variants=[
            V(
                D.YQL, lambda expr: sa.cast(sa.cast(sa.cast(expr, sa.BIGINT), sa.DATETIME), sa.DATE)
            ),  # number -> dt -> date
        ]
    ),
    # datetime
    base.FuncDatetime1FromNull.for_dialect(D.YQL),
    base.FuncDatetime1FromDatetime.for_dialect(D.YQL),
    base.FuncDatetime1FromDate.for_dialect(D.YQL),
    base.FuncDatetime1FromNumber(
        variants=[
            V(D.YQL, lambda expr: sa.cast(sa.cast(expr, sa.BIGINT), sa.DateTime())),
        ]
    ),
    base.FuncDatetime1FromString(
        variants=[
            # e.g. `DateTime::MakeDatetime(DateTime::ParseIso8601('2021-06-01 18:00:59')) as c`
            V(D.YQL, lambda expr: sa.func.DateTime.MakeDatetime(sa.func.DateTime.ParseIso8601(expr))),
        ]
    ),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.YQL),
    # float
    base.FuncFloatNumber(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.FLOAT)),  # TODO: need it to become SQL `CAST(… AS DOUBLE)`.
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(D.YQL, lambda expr: sa.cast(sa.cast(expr, sa.DATETIME), sa.FLOAT)),  # date -> dt -> number
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.YQL, lambda expr: sa.cast(expr, sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.YQL, lambda expr: sa.cast(expr, sa.FLOAT)),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull.for_dialect(D.YQL),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.YQL),
    base.FuncGenericDatetime1FromDate.for_dialect(D.YQL),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(D.YQL, lambda expr: sa.cast(sa.cast(expr, sa.BIGINT), sa.DateTime())),
        ]
    ),
    base.FuncGenericDatetime1FromString(
        variants=[
            # e.g. `DateTime::MakeDatetime(DateTime::ParseIso8601('2021-06-01 18:00:59')) as c`
            V(D.YQL, lambda expr: sa.func.DateTime.MakeDatetime(sa.func.DateTime.ParseIso8601(expr))),
        ]
    ),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.YQL),
    base.FuncGeopointFromCoords.for_dialect(D.YQL),
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.YQL),
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.YQL, lambda _: sa.cast(sa.null(), sa.BIGINT())),
        ]
    ),
    base.FuncIntFromInt.for_dialect(D.YQL),
    base.FuncIntFromFloat(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.BIGINT())),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.YQL, lambda expr: sa.func.cast(expr, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(D.YQL, lambda expr: sa.cast(sa.cast(expr, sa.DATETIME), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(D.YQL, lambda expr: sa.cast(expr, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(D.YQL, lambda expr: sa.cast(expr, sa.BIGINT)),
        ]
    ),
    # str
    base.FuncStrFromNull(
        variants=[
            V(D.YQL, lambda value: sa.cast(sa.null(), sa.TEXT)),
        ]
    ),
    base.FuncStrFromUnsupported(
        variants=[
            # YQL: uncertain.
            # Does not work for e.g. arrays:
            # V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),
            # Does not work for e.g. Decimal:
            V(
                D.YQL,
                lambda value: sa.cast(sa.func.ToBytes(sa.func.Yson.SerializePretty(sa.func.Yson.From(value))), sa.TEXT),
            ),
        ]
    ),
    base.FuncStrFromInteger(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(D.YQL, lambda value: sa.case(whens=[(value.is_(None), sa.null()), (value, "True")], else_="False")),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.YQL),
    base.FuncStrFromDate(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),  # results in e.g. "2021-06-01T15:20:24Z"
        ]
    ),
    base.FuncStrFromString.for_dialect(D.YQL),
    base.FuncStrFromUUID(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),
        ]
    ),
]
