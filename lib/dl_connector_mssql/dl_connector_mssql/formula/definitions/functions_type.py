import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common_datetime import EPOCH_START_S
import dl_formula.definitions.functions_type as base

from dl_connector_mssql.formula.constants import MssqlDialect as D


V = TranslationVariant.make


class MSSQLFuncBoolFromNumber(base.FuncBoolFromNumber):
    variants = [
        V(
            D.MSSQLSRV,
            lambda value: sa.case(whens=[(value.is_(None), sa.null()), (value != sa.literal(0), 1)], else_=0),
        ),
    ]
    return_flags = 0  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "int", base class "FuncBoolFromNumber" defined the type as "ContextFlag")  [assignment]


DEFINITIONS_TYPE = [
    # bool
    base.FuncBoolFromNull(
        variants=[
            V(D.MSSQLSRV, lambda _: sa.null()),
        ]
    ),
    MSSQLFuncBoolFromNumber(),
    base.FuncBoolFromBool.for_dialect(D.MSSQLSRV),
    base.FuncBoolFromStrGeo.for_dialect(D.MSSQLSRV),
    base.FuncBoolFromDateDatetime.for_dialect(D.MSSQLSRV),
    # date
    base.FuncDate1FromNull.for_dialect(D.MSSQLSRV),
    base.FuncDate1FromDatetime.for_dialect(D.MSSQLSRV),
    base.FuncDate1FromString.for_dialect(D.MSSQLSRV),
    base.FuncDate1FromNumber(
        variants=[
            V(D.MSSQLSRV, lambda expr: sa.cast(sa.func.DATEADD(sa.text("SECOND"), expr, EPOCH_START_S), sa.Date())),
        ]
    ),
    # datetime
    base.FuncDatetime1FromNull.for_dialect(D.MSSQLSRV),
    base.FuncDatetime1FromDatetime.for_dialect(D.MSSQLSRV),
    base.FuncDatetime1FromDate.for_dialect(D.MSSQLSRV),
    base.FuncDatetime1FromNumber(
        variants=[
            V(D.MSSQLSRV, lambda expr: sa.func.DATEADD(sa.text("SECOND"), expr, EPOCH_START_S)),
        ]
    ),
    base.FuncDatetime1FromString.for_dialect(D.MSSQLSRV),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.MSSQLSRV),
    # float
    base.FuncFloatNumber(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.cast(value, sa.FLOAT)),
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.cast(value, sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.cast(value, sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.cast(sa.func.DATEDIFF(sa.text("SECOND"), EPOCH_START_S, value), sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.cast(sa.func.DATEDIFF(sa.text("SECOND"), EPOCH_START_S, value), sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.cast(sa.func.DATEDIFF(sa.text("SECOND"), EPOCH_START_S, value), sa.FLOAT)),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull.for_dialect(D.MSSQLSRV),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.MSSQLSRV),
    base.FuncGenericDatetime1FromDate.for_dialect(D.MSSQLSRV),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(D.MSSQLSRV, lambda expr: sa.func.DATEADD(sa.text("SECOND"), expr, EPOCH_START_S)),
        ]
    ),
    base.FuncGenericDatetime1FromString.for_dialect(D.MSSQLSRV),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.MSSQLSRV),
    base.FuncGeopointFromCoords.for_dialect(D.MSSQLSRV),
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.MSSQLSRV),
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.MSSQLSRV, lambda _: sa.cast(sa.null(), sa.BIGINT())),
        ]
    ),
    base.FuncIntFromInt.for_dialect(D.MSSQLSRV),
    base.FuncIntFromFloat(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.cast(value, sa.BIGINT())),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            V(D.MSSQLSRV, lambda value: value),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.cast(value, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(
                D.MSSQLSRV,
                lambda value: sa.cast(sa.func.DATEDIFF(sa.text("SECOND"), EPOCH_START_S, value), sa.BIGINT()),
            ),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(
                D.MSSQLSRV,
                lambda value: sa.cast(sa.func.DATEDIFF(sa.text("SECOND"), EPOCH_START_S, value), sa.BIGINT()),
            ),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(
                D.MSSQLSRV,
                lambda value: sa.cast(sa.func.DATEDIFF(sa.text("SECOND"), EPOCH_START_S, value), sa.BIGINT()),
            ),
        ]
    ),
    # str
    base.FuncStrFromUnsupported(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.func.TRIM(sa.cast(value, sa.CHAR))),
        ]
    ),
    base.FuncStrFromInteger(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.func.TRIM(sa.cast(value, sa.CHAR))),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            # "128" is deprecated constant value, but it seems, currently it's the only
            # universal (for all mssqlserver versions) value to stringify float as is.
            # Also there is a little untidiness with float zero: STR(0.0) -> "0.0E0"
            # https://stackoverflow.com/questions/3715675/how-to-convert-float-to-varchar-in-sql-server/24909501#24909501
            V(D.MSSQLSRV, lambda value: sa.func.CONVERT(sa.text("VARCHAR"), value, 128)),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(
                D.MSSQLSRV,
                lambda value: sa.case(
                    whens=[(value.is_(None), sa.null()), (value != sa.literal(0), "True")], else_="False"
                ),
            ),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.MSSQLSRV),
    base.FuncStrFromDate(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.func.CONVERT(sa.text("VARCHAR"), value, 23)),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.func.CONVERT(sa.text("VARCHAR"), value, 120)),
        ]
    ),
    base.FuncStrFromString.for_dialect(D.MSSQLSRV),
    base.FuncStrFromUUID(
        variants=[
            V(D.MSSQLSRV, lambda value: sa.func.CONVERT(sa.text("VARCHAR"), value)),
        ]
    ),
]
