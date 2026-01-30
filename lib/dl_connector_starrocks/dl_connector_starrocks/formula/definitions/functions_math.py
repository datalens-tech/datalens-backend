import sqlalchemy as sa

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import dl_formula.definitions.functions_math as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


DEFINITIONS_MATH = [
    # abs
    base.FuncAbs.for_dialect(D.STARROCKS),
    # acos
    base.FuncAcos.for_dialect(D.STARROCKS),
    # asin
    base.FuncAsin.for_dialect(D.STARROCKS),
    # atan
    base.FuncAtan.for_dialect(D.STARROCKS),
    # atan2
    base.FuncAtan2.for_dialect(D.STARROCKS),
    # ceiling
    base.FuncCeiling(
        variants=[
            V(
                D.STARROCKS,
                lambda num: sa.func.ceil(num) + 0,
            )
        ]
    ),
    # cos
    base.FuncCos.for_dialect(D.STARROCKS),
    # cot
    base.FuncCot.for_dialect(D.STARROCKS),
    # degrees
    base.FuncDegrees.for_dialect(D.STARROCKS),
    # div
    base.FuncDivBasic(
        variants=[
            V(D.STARROCKS, lambda x, y: x.op("DIV")(y)),
        ]
    ),
    # div_safe
    base.FuncDivSafe2(
        variants=[
            V(D.STARROCKS, lambda x, y: sa.func.IF(y != 0, x.op("DIV")(y), None)),
        ]
    ),
    base.FuncDivSafe3(
        variants=[
            V(D.STARROCKS, lambda x, y, default: sa.func.IF(y != 0, x.op("DIV")(y), default)),
        ]
    ),
    # exp
    base.FuncExp.for_dialect(D.STARROCKS),
    # fdiv_safe
    base.FuncFDivSafe2.for_dialect(D.STARROCKS),
    base.FuncFDivSafe3.for_dialect(D.STARROCKS),
    # floor
    base.FuncFloor.for_dialect(D.STARROCKS),
    # greatest
    base.FuncGreatest1.for_dialect(D.STARROCKS),
    base.FuncGreatestMain.for_dialect(D.STARROCKS),
    base.GreatestMulti.for_dialect(D.STARROCKS),
    # least
    base.FuncLeast1.for_dialect(D.STARROCKS),
    base.FuncLeastMain.for_dialect(D.STARROCKS),
    base.LeastMulti.for_dialect(D.STARROCKS),
    # ln
    base.FuncLn.for_dialect(D.STARROCKS),
    # log
    base.FuncLog.for_dialect(D.STARROCKS),
    # log10
    base.FuncLog10.for_dialect(D.STARROCKS),
    # pi
    base.FuncPi.for_dialect(D.STARROCKS),
    # power
    base.FuncPower.for_dialect(D.STARROCKS),
    # radians
    base.FuncRadians.for_dialect(D.STARROCKS),
    # round
    base.FuncRound1(
        variants=[
            V(D.STARROCKS, lambda num: sa.func.round(num) + 0),
        ],
    ),
    base.FuncRound2(
        variants=[
            V(
                D.STARROCKS,
                lambda num, precision: sa.func.round(num, precision) + 0,
            )
        ]
    ),
    # sign
    base.FuncSign.for_dialect(D.STARROCKS),
    # sin
    base.FuncSin.for_dialect(D.STARROCKS),
    # sqrt
    base.FuncSqrt.for_dialect(D.STARROCKS),
    # square
    base.FuncSquare(
        variants=[
            V(D.STARROCKS, lambda x: sa.func.POW(x, 2)),
        ]
    ),
    # tan
    base.FuncTan.for_dialect(D.STARROCKS),
]
