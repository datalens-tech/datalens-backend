import sqlalchemy as sa
import sqlalchemy.dialects.mssql as sa_mssqlsrv

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_math as base
from dl_formula.definitions.type_strategy import ParamsFromArgs

from bi_connector_mssql.formula.constants import MssqlDialect as D


V = TranslationVariant.make


class FuncDivMSSQLInt(base.FuncDiv):
    variants = [V(D.MSSQLSRV, lambda x, y: x / y)]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.INTEGER]),
    ]


class FuncDivMSSQLFloat(base.FuncDiv):
    variants = [V(D.MSSQLSRV, lambda x, y: sa.cast(x / y, sa.BIGINT))]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]


class FuncDivSafe2MSSQLInt(base.FuncDivSafe2):
    variants = [V(D.MSSQLSRV, lambda x, y: sa.func.IIF(y != 0, x / y, None))]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.INTEGER]),
    ]


class FuncDivSafe3MSSQLInt(base.FuncDivSafe3):
    variants = [V(D.MSSQLSRV, lambda x, y, default: sa.func.IIF(y != 0, x / y, default))]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.INTEGER, DataType.INTEGER]),
    ]


class FuncDivSafe2MSSQLFloat(base.FuncDivSafe2):
    variants = [V(D.MSSQLSRV, lambda x, y: sa.func.IIF(y != 0, sa.cast(x / y, sa.BIGINT), None))]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]


class FuncDivSafe3MSSQLFloat(base.FuncDivSafe3):
    variants = [V(D.MSSQLSRV, lambda x, y, default: sa.func.IIF(y != 0, sa.cast(x / y, sa.BIGINT), default))]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT, DataType.INTEGER]),
    ]


class FuncGreatestMSSQL(base.FuncGreatestBase):
    variants = [V(D.MSSQLSRV, lambda x, y: sa.func.IIF(x >= y, x, y))]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
        ArgTypeSequence([DataType.BOOLEAN, DataType.BOOLEAN]),
    ]


class FuncGreatestDatesMSSQL(base.FuncGreatestBase):
    variants = [
        V(D.MSSQLSRV, lambda x, y: sa.cast(sa.func.IIF(x >= y, x, y), sa.Date)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.DATE]),
    ]


class FuncGreatestDatetimesMSSQL(base.FuncGreatestBase):
    variants = [
        V(D.MSSQLSRV, lambda x, y: sa.cast(sa.func.IIF(x >= y, x, y), sa.DateTime)),
    ]
    argument_types = [
        # Note: should not mix tz-naive and tz-aware datetimes together.
        ArgTypeSequence([DataType.DATETIME, DataType.DATETIME]),
        ArgTypeSequence([DataType.DATETIMETZ, DataType.DATETIMETZ]),
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
    ]
    return_type_params = ParamsFromArgs(0)


class FuncLeastMSSQL(base.FuncLeastBase):
    variants = [V(D.MSSQLSRV, lambda x, y: sa.func.IIF(x <= y, x, y))]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
        ArgTypeSequence([DataType.BOOLEAN, DataType.BOOLEAN]),
    ]


class FuncLeastDatesMSSQL(base.FuncLeastBase):
    variants = [
        V(D.MSSQLSRV, lambda x, y: sa.cast(sa.func.IIF(x <= y, x, y), sa.Date)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.DATE]),
    ]


class FuncLeastDatetimesMSSQL(base.FuncLeastBase):
    variants = [
        V(D.MSSQLSRV, lambda x, y: sa.cast(sa.func.IIF(x <= y, x, y), sa.DateTime)),
    ]
    argument_types = [
        # Note: should not mix tz-naive and tz-aware datetimes together.
        ArgTypeSequence([DataType.DATETIME, DataType.DATETIME]),
        ArgTypeSequence([DataType.DATETIMETZ, DataType.DATETIMETZ]),
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
    ]
    return_type_params = ParamsFromArgs(0)


DEFINITIONS_MATH = [
    # abs
    base.FuncAbs.for_dialect(D.MSSQLSRV),
    # acos
    base.FuncAcos.for_dialect(D.MSSQLSRV),
    # asin
    base.FuncAsin.for_dialect(D.MSSQLSRV),
    # atan
    base.FuncAtan.for_dialect(D.MSSQLSRV),
    # atan2
    base.FuncAtan2(
        variants=[
            V(D.MSSQLSRV, sa.func.ATN2),
        ]
    ),
    # ceiling
    base.FuncCeiling(
        variants=[
            V(D.MSSQLSRV, sa.func.CEILING),
        ]
    ),
    # cos
    base.FuncCos.for_dialect(D.MSSQLSRV),
    # cot
    base.FuncCot.for_dialect(D.MSSQLSRV),
    # degrees
    base.FuncDegrees.for_dialect(D.MSSQLSRV),
    # div
    FuncDivMSSQLInt(),
    FuncDivMSSQLFloat(),
    # div_safe
    FuncDivSafe2MSSQLInt(),
    FuncDivSafe3MSSQLInt(),
    FuncDivSafe2MSSQLFloat(),
    FuncDivSafe3MSSQLFloat(),
    # exp
    base.FuncExp.for_dialect(D.MSSQLSRV),
    # fdiv_safe
    base.FuncFDivSafe2.for_dialect(D.MSSQLSRV),
    base.FuncFDivSafe3.for_dialect(D.MSSQLSRV),
    # floor
    base.FuncFloor.for_dialect(D.MSSQLSRV),
    # greatest
    base.FuncGreatest1.for_dialect(D.MSSQLSRV),
    FuncGreatestMSSQL(),
    FuncGreatestDatesMSSQL(),
    FuncGreatestDatetimesMSSQL(),
    base.GreatestMulti.for_dialect(D.MSSQLSRV),
    # least
    base.FuncLeast1.for_dialect(D.MSSQLSRV),
    FuncLeastMSSQL(),
    FuncLeastDatesMSSQL(),
    FuncLeastDatetimesMSSQL(),
    base.LeastMulti.for_dialect(D.MSSQLSRV),
    # ln
    base.FuncLn(
        variants=[
            V(D.MSSQLSRV, sa.func.LOG),
        ]
    ),
    # log
    base.FuncLog(
        variants=[
            V(D.MSSQLSRV, lambda x, base: sa.func.LOG(x, base)),
        ]
    ),
    # log10
    base.FuncLog10.for_dialect(D.MSSQLSRV),
    # pi
    base.FuncPi.for_dialect(D.MSSQLSRV),
    # power
    base.FuncPower(
        variants=[
            V(D.MSSQLSRV, lambda x, y: sa.func.POWER(sa.cast(x, sa_mssqlsrv.FLOAT), sa.cast(y, sa_mssqlsrv.FLOAT))),
        ]
    ),
    # radians
    base.FuncRadians(
        variants=[
            V(D.MSSQLSRV, lambda x: sa.func.RADIANS(sa.cast(x, sa_mssqlsrv.FLOAT))),
        ]
    ),
    # round
    base.FuncRound1(
        variants=[
            V(D.MSSQLSRV, lambda x: sa.func.ROUND(x, 0)),
        ]
    ),
    base.FuncRound2.for_dialect(D.MSSQLSRV),
    # sign
    base.FuncSign.for_dialect(D.MSSQLSRV),
    # sin
    base.FuncSin.for_dialect(D.MSSQLSRV),
    # sqrt
    base.FuncSqrt.for_dialect(D.MSSQLSRV),
    # square
    base.FuncSquare(
        variants=[
            V(D.MSSQLSRV, sa.func.SQUARE),
        ]
    ),
    # tan
    base.FuncTan.for_dialect(D.MSSQLSRV),
]
