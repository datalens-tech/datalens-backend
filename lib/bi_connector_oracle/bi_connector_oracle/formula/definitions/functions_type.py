import sqlalchemy as sa
import sqlalchemy.dialects.oracle.base as sa_oracle

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common_datetime import (
    DAY_SEC,
    EPOCH_START_D,
)
import dl_formula.definitions.functions_type as base

from bi_connector_oracle.formula.constants import OracleDialect as D


V = TranslationVariant.make


DEFINITIONS_TYPE = [
    # bool
    base.FuncBoolFromNull(
        variants=[
            V(D.ORACLE, lambda _: sa.cast(sa.null(), sa_oracle.INTEGER())),
        ]
    ),
    base.FuncBoolFromNumber.for_dialect(D.ORACLE),
    base.FuncBoolFromBool.for_dialect(D.ORACLE),
    base.FuncBoolFromStrGeo(
        variants=[
            V(D.ORACLE, lambda value: value.isnot(None)),
        ]
    ),
    base.FuncBoolFromDateDatetime.for_dialect(D.ORACLE),
    # date
    base.FuncDate1FromNull.for_dialect(D.ORACLE),
    base.FuncDate1FromDatetime(
        variants=[
            V(D.ORACLE, lambda expr: expr),
        ]
    ),
    base.FuncDate1FromString(
        variants=[
            V(D.ORACLE, lambda expr: sa.func.TO_DATE(expr, "YYYY-MM-DD")),
        ]
    ),
    base.FuncDate1FromNumber(
        variants=[
            V(D.ORACLE, lambda expr: EPOCH_START_D + expr / DAY_SEC),
        ]
    ),
    # datetime
    base.FuncDatetime1FromNull(
        variants=[
            V(D.ORACLE, lambda _: sa.cast(sa.null(), sa.Date())),
        ]
    ),
    base.FuncDatetime1FromDatetime.for_dialect(D.ORACLE),
    base.FuncDatetime1FromDate(
        variants=[
            V(D.ORACLE, lambda expr: expr),
        ]
    ),
    base.FuncDatetime1FromNumber(
        variants=[
            V(D.ORACLE, lambda expr: EPOCH_START_D + expr / DAY_SEC),
        ]
    ),
    base.FuncDatetime1FromString(
        variants=[
            V(D.ORACLE, lambda expr: sa.func.TO_DATE(expr, "YYYY-MM-DD HH:MI:SS")),
        ]
    ),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.ORACLE),
    # float
    base.FuncFloatNumber(
        variants=[
            V(D.ORACLE, lambda value: sa.cast(value, sa_oracle.BINARY_DOUBLE)),
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.ORACLE, lambda value: sa.cast(value, sa_oracle.BINARY_DOUBLE)),
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(D.ORACLE, lambda value: sa.cast(value, sa_oracle.BINARY_DOUBLE)),
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(D.ORACLE, lambda value: sa.type_coerce((value - EPOCH_START_D) * DAY_SEC, sa_oracle.BINARY_DOUBLE)),
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.ORACLE, lambda value: sa.type_coerce((value - EPOCH_START_D) * DAY_SEC, sa_oracle.BINARY_DOUBLE)),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.ORACLE, lambda value: sa.type_coerce((value - EPOCH_START_D) * DAY_SEC, sa_oracle.BINARY_DOUBLE)),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull(
        variants=[
            V(D.ORACLE, lambda _: sa.cast(sa.null(), sa.Date())),
        ]
    ),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.ORACLE),
    base.FuncGenericDatetime1FromDate(
        variants=[
            V(D.ORACLE, lambda expr: expr),
        ]
    ),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(D.ORACLE, lambda expr: EPOCH_START_D + expr / DAY_SEC),
        ]
    ),
    base.FuncGenericDatetime1FromString(
        variants=[
            V(D.ORACLE, lambda expr: sa.func.TO_DATE(expr, "YYYY-MM-DD HH:MI:SS")),
        ]
    ),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.ORACLE),
    base.FuncGeopointFromCoords.for_dialect(D.ORACLE),
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.ORACLE),
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.ORACLE, lambda _: sa.cast(sa.null(), sa_oracle.INTEGER())),
        ]
    ),
    base.FuncIntFromInt.for_dialect(D.ORACLE),
    base.FuncIntFromFloat(
        variants=[
            V(D.ORACLE, lambda value: sa.cast(sa.func.FLOOR(value), sa.Integer)),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            V(D.ORACLE, lambda value: value),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.ORACLE, lambda value: sa.cast(value, sa.Integer)),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(D.ORACLE, lambda value: sa.cast((value - EPOCH_START_D) * DAY_SEC, sa.Integer)),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(D.ORACLE, lambda value: sa.cast((value - EPOCH_START_D) * DAY_SEC, sa.Integer)),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(D.ORACLE, lambda value: sa.cast((value - EPOCH_START_D) * DAY_SEC, sa.Integer)),
        ]
    ),
    # str
    base.FuncStrFromUnsupported(
        variants=[
            V(D.ORACLE, sa.func.TO_CHAR),
        ]
    ),
    base.FuncStrFromInteger(
        variants=[
            V(D.ORACLE, sa.func.TO_CHAR),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            V(D.ORACLE, sa.func.TO_CHAR),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(
                D.ORACLE,
                lambda value: sa.case(
                    whens=[(value.is_(None), sa.null()), (value != sa.literal(0), "True")], else_="False"
                ),
            ),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.ORACLE),
    base.FuncStrFromDate(
        variants=[
            V(D.ORACLE, lambda value: sa.func.TO_CHAR(value, "YYYY-MM-DD")),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(D.ORACLE, lambda value: sa.func.TO_CHAR(value, "YYYY-MM-DD HH:MI:SS")),
        ]
    ),
    base.FuncStrFromString.for_dialect(D.ORACLE),
]
