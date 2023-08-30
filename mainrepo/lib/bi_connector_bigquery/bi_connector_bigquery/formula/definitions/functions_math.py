import math

import sqlalchemy as sa

import bi_formula.definitions.functions_math as base
from bi_formula.definitions.base import TranslationVariant

from bi_connector_bigquery.formula.constants import BigQueryDialect as D


V = TranslationVariant.make

DEFINITIONS_MATH = [
    # abs
    base.FuncAbs.for_dialect(D.BIGQUERY),

    # acos
    base.FuncAcos.for_dialect(D.BIGQUERY),

    # asin
    base.FuncAsin.for_dialect(D.BIGQUERY),

    # atan
    base.FuncAtan.for_dialect(D.BIGQUERY),

    # atan2
    base.FuncAtan2.for_dialect(D.BIGQUERY),

    # ceiling
    base.FuncCeiling.for_dialect(D.BIGQUERY),

    # cos
    base.FuncCos.for_dialect(D.BIGQUERY),

    # cot
    base.FuncCot.for_dialect(D.BIGQUERY),

    # degrees
    base.FuncDegrees(variants=[
        V(D.BIGQUERY, lambda x: x / math.pi * 180),
    ]),

    # div
    base.FuncDivBasic.for_dialect(D.BIGQUERY),

    # div_safe
    base.FuncDivSafe2.for_dialect(D.BIGQUERY),
    base.FuncDivSafe3.for_dialect(D.BIGQUERY),

    # exp
    base.FuncExp.for_dialect(D.BIGQUERY),

    # floor
    base.FuncFloor.for_dialect(D.BIGQUERY),

    # greatest
    base.FuncGreatest1.for_dialect(D.BIGQUERY),
    base.FuncGreatestMain.for_dialect(D.BIGQUERY),
    # base.GreatestMulti.for_dialect(D.BIGQUERY),  # FIXME
    base.GreatestMulti(variants=[
        V(D.BIGQUERY, sa.func.GREATEST),
    ]),

    # least
    base.FuncLeast1.for_dialect(D.BIGQUERY),
    base.FuncLeastMain.for_dialect(D.BIGQUERY),
    # base.LeastMulti.for_dialect(D.BIGQUERY),  # FIXME
    base.LeastMulti(variants=[
        V(D.BIGQUERY, sa.func.LEAST),
    ]),

    # ln
    base.FuncLn.for_dialect(D.BIGQUERY),

    # log
    base.FuncLog(variants=[
        V(D.BIGQUERY, lambda x, base: sa.func.LOG(x, base)),
    ]),

    # log10
    base.FuncLog10.for_dialect(D.BIGQUERY),

    # pi
    base.FuncPi(variants=[
        V(D.BIGQUERY, lambda: sa.literal(math.pi)),
    ]),

    # power
    base.FuncPower(variants=[
        V(D.BIGQUERY, sa.func.POWER),
    ]),

    # radians
    base.FuncRadians(variants=[
        V(D.BIGQUERY, lambda x: x * math.pi / 180),
    ]),

    # round
    base.FuncRound1.for_dialect(D.BIGQUERY),
    base.FuncRound2.for_dialect(D.BIGQUERY),

    # sign
    base.FuncSign.for_dialect(D.BIGQUERY),

    # sin
    base.FuncSin.for_dialect(D.BIGQUERY),

    # sqrt
    base.FuncSqrt.for_dialect(D.BIGQUERY),

    # square
    base.FuncSquare(variants=[
        V(D.BIGQUERY, lambda x: sa.func.POW(x, 2)),
    ]),

    # tan
    base.FuncTan.for_dialect(D.BIGQUERY),
]
