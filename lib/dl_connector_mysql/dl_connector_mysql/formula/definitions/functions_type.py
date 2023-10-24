import sqlalchemy as sa
import sqlalchemy.dialects.mysql as sa_mysql

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_type as base

from dl_connector_mysql.formula.constants import MySQLDialect as D


V = TranslationVariant.make


DEFINITIONS_TYPE = [
    # bool
    base.FuncBoolFromNull(
        variants=[
            V(D.MYSQL, lambda _: sa.cast(sa.null(), sa_mysql.TINYINT())),
        ]
    ),
    base.FuncBoolFromNumber.for_dialect(D.MYSQL),
    base.FuncBoolFromBool.for_dialect(D.MYSQL),
    base.FuncBoolFromStrGeo.for_dialect(D.MYSQL),
    base.FuncBoolFromDateDatetime.for_dialect(D.MYSQL),
    # date
    base.FuncDate1FromNull.for_dialect(D.MYSQL),
    base.FuncDate1FromDatetime.for_dialect(D.MYSQL),
    base.FuncDate1FromString.for_dialect(D.MYSQL),
    base.FuncDate1FromNumber(
        variants=[
            V(D.MYSQL, lambda expr: sa.cast(sa.func.FROM_UNIXTIME(expr), sa.Date())),
        ]
    ),
    # datetime
    base.FuncDatetime1FromNull.for_dialect(D.MYSQL),
    base.FuncDatetime1FromDatetime.for_dialect(D.MYSQL),
    base.FuncDatetime1FromDate.for_dialect(D.MYSQL),
    base.FuncDatetime1FromNumber(
        variants=[
            V(D.MYSQL, lambda expr: sa.func.FROM_UNIXTIME(expr)),
        ]
    ),
    base.FuncDatetime1FromString.for_dialect(D.MYSQL),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.MYSQL),
    # float
    base.FuncFloatNumber(
        variants=[
            V(D.MYSQL, lambda value: value + sa.literal(0.0)),  # explicit cast to float is not supported
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.MYSQL, lambda value: value + sa.literal(0.0)),  # explicit cast to float is not supported
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(D.MYSQL, lambda value: value + sa.literal(0.0)),  # explicit cast to float is not supported
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(sa.func.UNIX_TIMESTAMP(value), sa_mysql.FLOAT)),
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(sa.func.UNIX_TIMESTAMP(value), sa_mysql.FLOAT)),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(sa.func.UNIX_TIMESTAMP(value), sa_mysql.FLOAT)),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull.for_dialect(D.MYSQL),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.MYSQL),
    base.FuncGenericDatetime1FromDate.for_dialect(D.MYSQL),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(D.MYSQL, lambda expr: sa.func.FROM_UNIXTIME(expr)),
        ]
    ),
    base.FuncGenericDatetime1FromString.for_dialect(D.MYSQL),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.MYSQL),
    base.FuncGeopointFromCoords.for_dialect(D.MYSQL),
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.MYSQL),
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.MYSQL, lambda _: sa.cast(sa.null(), sa_mysql.BIGINT())),
        ]
    ),
    base.FuncIntFromInt.for_dialect(D.MYSQL),
    base.FuncIntFromFloat(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(sa.func.FLOOR(value), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            V(D.MYSQL, lambda value: value),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(value, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(D.MYSQL, sa.func.UNIX_TIMESTAMP),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(sa.func.UNIX_TIMESTAMP(value), sa_mysql.BIGINT)),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(sa.func.UNIX_TIMESTAMP(value), sa_mysql.BIGINT)),
        ]
    ),
    # str
    base.FuncStrFromUnsupported(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(value, sa.CHAR)),
        ]
    ),
    base.FuncStrFromInteger(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(value, sa.CHAR)),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(value, sa.CHAR)),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(D.MYSQL, lambda value: sa.case(whens=[(value.is_(None), sa.null()), (value, "True")], else_="False")),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.MYSQL),
    base.FuncStrFromDate(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(value, sa.CHAR)),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(D.MYSQL, lambda value: sa.cast(value, sa.CHAR)),
        ]
    ),
    base.FuncStrFromString.for_dialect(D.MYSQL),
]
