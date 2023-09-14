import sqlalchemy as sa

from bi_formula.definitions.base import TranslationVariant
import bi_formula.definitions.functions_type as base

from bi_connector_bigquery.formula.constants import BigQueryDialect as D

V = TranslationVariant.make


DEFINITIONS_TYPE = [
    # bool
    base.FuncBoolFromNull.for_dialect(D.BIGQUERY),
    base.FuncBoolFromNumber.for_dialect(D.BIGQUERY),
    base.FuncBoolFromBool.for_dialect(D.BIGQUERY),
    base.FuncBoolFromStrGeo.for_dialect(D.BIGQUERY),
    base.FuncBoolFromDateDatetime.for_dialect(D.BIGQUERY),
    # date
    base.FuncDate1FromNull.for_dialect(D.BIGQUERY),
    base.FuncDate1FromDatetime.for_dialect(D.BIGQUERY),
    base.FuncDate1FromString.for_dialect(D.BIGQUERY),
    base.FuncDate1FromNumber(
        variants=[V(D.BIGQUERY, lambda expr: sa.cast(sa.func.TIMESTAMP_SECONDS(sa.cast(expr, sa.INTEGER)), sa.DATE))]
    ),
    # datetime
    base.FuncDatetime1FromNull.for_dialect(D.BIGQUERY),
    base.FuncDatetime1FromDatetime.for_dialect(D.BIGQUERY),
    base.FuncDatetime1FromDate.for_dialect(D.BIGQUERY),
    base.FuncDatetime1FromNumber(
        variants=[
            V(D.BIGQUERY, lambda expr: sa.cast(sa.func.TIMESTAMP_SECONDS(sa.cast(expr, sa.INTEGER)), sa.DATETIME))
        ]
    ),
    base.FuncDatetime1FromString.for_dialect(D.BIGQUERY),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.BIGQUERY),
    # float
    base.FuncFloatNumber(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.FLOAT)),
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.IF(value, 1.0, 0.0)),
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(sa.func.UNIX_SECONDS(sa.cast(value, sa.TIMESTAMP)), sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(sa.func.UNIX_SECONDS(sa.cast(value, sa.TIMESTAMP)), sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(sa.func.UNIX_SECONDS(sa.cast(value, sa.TIMESTAMP)), sa.FLOAT)),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull.for_dialect(D.BIGQUERY),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.BIGQUERY),
    base.FuncGenericDatetime1FromDate.for_dialect(D.BIGQUERY),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(D.BIGQUERY, lambda expr: sa.cast(sa.func.TIMESTAMP_SECONDS(sa.cast(expr, sa.INTEGER)), sa.DATETIME))
        ]
    ),
    base.FuncGenericDatetime1FromString.for_dialect(D.BIGQUERY),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.BIGQUERY),
    base.FuncGeopointFromCoords.for_dialect(D.BIGQUERY),
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.BIGQUERY),
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.INTEGER)),
        ]
    ),
    base.FuncIntFromInt(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.INTEGER)),
        ]
    ),
    base.FuncIntFromFloat(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(sa.func.FLOOR(value), sa.INTEGER)),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.IF(value, 1, 0)),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.INTEGER)),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.UNIX_SECONDS(sa.cast(value, sa.TIMESTAMP))),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.UNIX_SECONDS(sa.cast(value, sa.TIMESTAMP))),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.UNIX_SECONDS(sa.cast(value, sa.TIMESTAMP))),
        ]
    ),
    base.FuncIntFromDatetimeTZ(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.UNIX_SECONDS(sa.cast(value, sa.TIMESTAMP))),
        ]
    ),
    # str
    base.FuncStrFromNull(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(sa.null(), sa.String())),
        ]
    ),
    base.FuncStrFromUnsupported(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    base.FuncStrFromInteger(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(D.BIGQUERY, lambda value: sa.case(whens=[(value.is_(None), sa.null()), (value, "True")], else_="False")),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.BIGQUERY),
    base.FuncStrFromDate(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    base.FuncStrFromDatetimeTZ(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    base.FuncStrFromString.for_dialect(D.BIGQUERY),
    base.FuncStrFromUUID(
        variants=[
            V(D.BIGQUERY, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    # base.FuncStrFromArray,  # FIXME
]
