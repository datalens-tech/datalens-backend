import math

import sqlalchemy as sa

from bi_formula.core.datatype import DataType
from bi_formula.definitions.args import ArgTypeSequence
from bi_formula.definitions.base import TranslationVariant
import bi_formula.definitions.functions_math as base

from bi_connector_snowflake.formula.constants import SnowFlakeDialect as D

V = TranslationVariant.make


class FuncDivSF(base.FuncDiv):
    variants = [V(D.SNOWFLAKE, lambda x, y: sa.cast(sa.func.trunc(x / y, 0), sa.BIGINT))]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.INTEGER, DataType.FLOAT]),
        ArgTypeSequence([DataType.FLOAT, DataType.INTEGER]),
    ]


class FuncDivSafe2SF(base.FuncDivSafe2):
    variants = [
        V(
            D.SNOWFLAKE,
            lambda x, y: sa.func.IFF(
                y != 0,
                sa.cast(sa.func.trunc(x / y, 0), sa.BIGINT),
                None,
            ),
        )
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.INTEGER, DataType.FLOAT]),
        ArgTypeSequence([DataType.FLOAT, DataType.INTEGER]),
    ]


class FuncDivSafe3SF(base.FuncDivSafe3):
    variants = [
        V(
            D.SNOWFLAKE,
            lambda x, y, default: sa.func.IFF(
                y != 0,
                sa.cast(sa.func.trunc(x / y, 0), sa.BIGINT),
                default,
            ),
        )
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.INTEGER, DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT, DataType.INTEGER]),
        ArgTypeSequence([DataType.INTEGER, DataType.FLOAT, DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT, DataType.INTEGER, DataType.INTEGER]),
    ]


DEFINITIONS_MATH = [
    # abs
    base.FuncAbs.for_dialect(D.SNOWFLAKE),
    # acos
    base.FuncAcos.for_dialect(D.SNOWFLAKE),
    # asin
    base.FuncAsin.for_dialect(D.SNOWFLAKE),
    # atan
    base.FuncAtan.for_dialect(D.SNOWFLAKE),
    # atan2
    base.FuncAtan2.for_dialect(D.SNOWFLAKE),
    # ceiling
    base.FuncCeiling.for_dialect(D.SNOWFLAKE),
    # cos
    base.FuncCos.for_dialect(D.SNOWFLAKE),
    # cot
    base.FuncCot.for_dialect(D.SNOWFLAKE),
    # degrees
    base.FuncDegrees(
        variants=[
            V(D.SNOWFLAKE, lambda x: x / math.pi * 180),
        ]
    ),
    # div
    FuncDivSF(),
    # div_safe
    FuncDivSafe2SF(),
    FuncDivSafe3SF(),
    # exp
    base.FuncExp.for_dialect(D.SNOWFLAKE),
    # fdiv_safe
    base.FuncFDivSafe2.for_dialect(D.SNOWFLAKE),
    base.FuncFDivSafe3.for_dialect(D.SNOWFLAKE),
    # floor
    base.FuncFloor.for_dialect(D.SNOWFLAKE),
    # greatest
    base.FuncGreatest1.for_dialect(D.SNOWFLAKE),
    base.FuncGreatestMain.for_dialect(D.SNOWFLAKE),
    # base.GreatestMulti.for_dialect(D.SNOWFLAKE),  # FIXME
    base.GreatestMulti(
        variants=[
            V(D.SNOWFLAKE, sa.func.GREATEST),
        ]
    ),
    # least
    base.FuncLeast1.for_dialect(D.SNOWFLAKE),
    base.FuncLeastMain.for_dialect(D.SNOWFLAKE),
    # base.LeastMulti.for_dialect(D.SNOWFLAKE),  # FIXME
    base.LeastMulti(
        variants=[
            V(D.SNOWFLAKE, sa.func.LEAST),
        ]
    ),
    # ln
    base.FuncLn.for_dialect(D.SNOWFLAKE),
    # log
    base.FuncLog(
        variants=[
            V(D.SNOWFLAKE, lambda x, base: sa.func.LOG(base, x)),
        ]
    ),
    # log10
    base.FuncLog10(
        variants=[
            V(D.SNOWFLAKE, lambda x: sa.func.LOG(10, x)),
        ]
    ),
    # pi
    base.FuncPi(
        variants=[
            V(D.SNOWFLAKE, lambda: sa.literal(math.pi)),
        ]
    ),
    # power
    base.FuncPower(
        variants=[
            V(D.SNOWFLAKE, sa.func.POWER),
        ]
    ),
    # radians
    base.FuncRadians(
        variants=[
            V(D.SNOWFLAKE, lambda x: x * math.pi / 180),
        ]
    ),
    # round
    base.FuncRound1.for_dialect(D.SNOWFLAKE),
    base.FuncRound2.for_dialect(D.SNOWFLAKE),
    # sign
    base.FuncSign.for_dialect(D.SNOWFLAKE),
    # sin
    base.FuncSin.for_dialect(D.SNOWFLAKE),
    # sqrt
    base.FuncSqrt.for_dialect(D.SNOWFLAKE),
    # square
    base.FuncSquare(
        variants=[
            V(D.SNOWFLAKE, lambda x: sa.func.POW(x, 2)),
        ]
    ),
    # tan
    base.FuncTan.for_dialect(D.SNOWFLAKE),
]
