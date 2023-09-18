import math

import sqlalchemy as sa

import dl_formula.definitions.functions_math as base
from dl_formula.definitions.base import TranslationVariant

from bi_connector_oracle.formula.constants import OracleDialect as D


V = TranslationVariant.make


DEFINITIONS_MATH = [
    # abs
    base.FuncAbs.for_dialect(D.ORACLE),

    # acos
    base.FuncAcos.for_dialect(D.ORACLE),

    # asin
    base.FuncAsin.for_dialect(D.ORACLE),

    # atan
    base.FuncAtan.for_dialect(D.ORACLE),

    # atan2
    base.FuncAtan2.for_dialect(D.ORACLE),

    # ceiling
    base.FuncCeiling.for_dialect(D.ORACLE),

    # cos
    base.FuncCos.for_dialect(D.ORACLE),

    # cot
    base.FuncCot(variants=[
        V(D.ORACLE, lambda x: sa.func.COS(x) / sa.func.SIN(x)),
    ]),

    # degrees
    base.FuncDegrees(variants=[
        V(D.ORACLE, lambda x: x / math.pi * 180),
    ]),

    # div
    base.FuncDivBasic(variants=[
        V(D.ORACLE, lambda x, y: sa.func.TRUNC(x / y)),
    ]),

    # div_safe
    base.FuncDivSafe2(variants=[
        V(D.ORACLE, lambda x, y: sa.func.TRUNC(x / sa.func.nullif(y, 0))),
    ]),
    base.FuncDivSafe3(variants=[
        V(D.ORACLE, lambda x, y, default: sa.func.coalesce(sa.func.TRUNC(x / sa.func.nullif(y, 0)), default)),
    ]),

    # exp
    base.FuncExp.for_dialect(D.ORACLE),

    # fdiv_safe
    base.FuncFDivSafe2.for_dialect(D.ORACLE),
    base.FuncFDivSafe3.for_dialect(D.ORACLE),

    # floor
    base.FuncFloor.for_dialect(D.ORACLE),

    # greatest
    base.FuncGreatest1.for_dialect(D.ORACLE),
    base.FuncGreatestMain.for_dialect(D.ORACLE),
    base.GreatestMulti.for_dialect(D.ORACLE),

    # least
    base.FuncLeast1.for_dialect(D.ORACLE),
    base.FuncLeastMain.for_dialect(D.ORACLE),
    base.LeastMulti.for_dialect(D.ORACLE),

    # ln
    base.FuncLn.for_dialect(D.ORACLE),

    # log
    base.FuncLog.for_dialect(D.ORACLE),

    # log10
    base.FuncLog10(variants=[
        V(D.ORACLE, lambda x: sa.func.LOG(10, x)),
    ]),

    # pi
    base.FuncPi(variants=[
        V(D.ORACLE, lambda: sa.literal(math.pi)),
    ]),

    # power
    base.FuncPower.for_dialect(D.ORACLE),

    # radians
    base.FuncRadians(variants=[
        V(D.ORACLE, lambda x: x * math.pi / 180),
    ]),

    # round
    base.FuncRound1.for_dialect(D.ORACLE),
    base.FuncRound2.for_dialect(D.ORACLE),

    # sign
    base.FuncSign.for_dialect(D.ORACLE),

    # sin
    base.FuncSin.for_dialect(D.ORACLE),

    # sqrt
    base.FuncSqrt.for_dialect(D.ORACLE),

    # square
    base.FuncSquare(variants=[
        V(D.ORACLE, lambda x: sa.func.POWER(x, 2)),
    ]),

    # tan
    base.FuncTan.for_dialect(D.ORACLE),
]
