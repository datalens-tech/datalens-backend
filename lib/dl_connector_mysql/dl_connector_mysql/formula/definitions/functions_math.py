import sqlalchemy as sa

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import dl_formula.definitions.functions_math as base

from dl_connector_mysql.formula.constants import MySQLDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


DEFINITIONS_MATH = [
    # abs
    base.FuncAbs.for_dialect(D.MYSQL),
    # acos
    base.FuncAcos.for_dialect(D.MYSQL),
    # asin
    base.FuncAsin.for_dialect(D.MYSQL),
    # atan
    base.FuncAtan.for_dialect(D.MYSQL),
    # atan2
    base.FuncAtan2.for_dialect(D.MYSQL),
    # ceiling
    base.FuncCeiling.for_dialect(D.MYSQL),
    # cos
    base.FuncCos.for_dialect(D.MYSQL),
    # cot
    base.FuncCot.for_dialect(D.MYSQL),
    # degrees
    base.FuncDegrees.for_dialect(D.MYSQL),
    # div
    base.FuncDivBasic(
        variants=[
            V(D.MYSQL, lambda x, y: x.op("DIV")(y)),
        ]
    ),
    # div_safe
    base.FuncDivSafe2(
        variants=[
            V(D.MYSQL, lambda x, y: sa.func.IF(y != 0, x.op("DIV")(y), None)),
        ]
    ),
    base.FuncDivSafe3(
        variants=[
            V(D.MYSQL, lambda x, y, default: sa.func.IF(y != 0, x.op("DIV")(y), default)),
        ]
    ),
    # exp
    base.FuncExp.for_dialect(D.MYSQL),
    # fdiv_safe
    base.FuncFDivSafe2.for_dialect(D.MYSQL),
    base.FuncFDivSafe3.for_dialect(D.MYSQL),
    # floor
    base.FuncFloor.for_dialect(D.MYSQL),
    # greatest
    base.FuncGreatest1.for_dialect(D.MYSQL),
    base.FuncGreatestMain.for_dialect(D.MYSQL),
    base.GreatestMulti.for_dialect(D.MYSQL),
    # least
    base.FuncLeast1.for_dialect(D.MYSQL),
    base.FuncLeastMain.for_dialect(D.MYSQL),
    base.LeastMulti.for_dialect(D.MYSQL),
    # ln
    base.FuncLn.for_dialect(D.MYSQL),
    # log
    base.FuncLog.for_dialect(D.MYSQL),
    # log10
    base.FuncLog10.for_dialect(D.MYSQL),
    # pi
    base.FuncPi.for_dialect(D.MYSQL),
    # power
    base.FuncPower.for_dialect(D.MYSQL),
    # radians
    base.FuncRadians.for_dialect(D.MYSQL),
    # round
    base.FuncRound1.for_dialect(D.MYSQL),
    base.FuncRound2.for_dialect(D.MYSQL),
    # sign
    base.FuncSign.for_dialect(D.MYSQL),
    # sin
    base.FuncSin.for_dialect(D.MYSQL),
    # sqrt
    base.FuncSqrt.for_dialect(D.MYSQL),
    # square
    base.FuncSquare(
        variants=[
            V(D.MYSQL, lambda x: sa.func.POW(x, 2)),
        ]
    ),
    # tan
    base.FuncTan.for_dialect(D.MYSQL),
]
