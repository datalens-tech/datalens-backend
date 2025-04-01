from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_math as base
from dl_formula.shortcuts import n

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make

DEFINITIONS_MATH = [
    # # abs
    # base.FuncAbs.for_dialect(D.TRINO),
    # # acos
    base.FuncAcos.for_dialect(D.TRINO),
    # # asin
    base.FuncAsin.for_dialect(D.TRINO),
    # # atan
    base.FuncAtan.for_dialect(D.TRINO),
    # # atan2
    base.FuncAtan2.for_dialect(D.TRINO),
    # # ceiling
    # base.FuncCeiling.for_dialect(D.TRINO),
    # # cos
    base.FuncCos.for_dialect(D.TRINO),
    # # cot
    base.FuncCot(
        variants=[
            V(D.TRINO, lambda x: n.func.POWER(n.func.TAN(x), -1)),
        ]
    ),
    # # degrees
    base.FuncDegrees.for_dialect(D.TRINO),
    # # div
    # base.FuncDivBasic.for_dialect(D.TRINO),
    # # div_safe
    # base.FuncDivSafe2.for_dialect(D.TRINO),
    # base.FuncDivSafe3.for_dialect(D.TRINO),
    # # exp
    # base.FuncExp.for_dialect(D.TRINO),
    # # floor
    # base.FuncFloor.for_dialect(D.TRINO),
    # # greatest
    # base.FuncGreatest1.for_dialect(D.TRINO),
    # base.FuncGreatestMain.for_dialect(D.TRINO),
    # base.GreatestMulti.for_dialect(D.TRINO),
    # # least
    # base.FuncLeast1.for_dialect(D.TRINO),
    # base.FuncLeastMain.for_dialect(D.TRINO),
    # base.LeastMulti.for_dialect(D.TRINO),
    # # ln
    # base.FuncLn.for_dialect(D.TRINO),
    # # log
    # base.FuncLog.for_dialect(D.TRINO),
    # # log10
    # base.FuncLog10.for_dialect(D.TRINO),
    # # pi
    base.FuncPi.for_dialect(D.TRINO),
    # # power
    base.FuncPower.for_dialect(D.TRINO),
    # # radians
    base.FuncRadians.for_dialect(D.TRINO),
    # # round
    base.FuncRound1.for_dialect(D.TRINO),
    # base.FuncRound2.for_dialect(D.TRINO),
    # # sign
    # base.FuncSign.for_dialect(D.TRINO),
    # # sin
    base.FuncSin.for_dialect(D.TRINO),
    # # sqrt
    # base.FuncSqrt.for_dialect(D.TRINO),
    # # square
    # base.FuncSquare.for_dialect(D.TRINO),
    # # tan
    base.FuncTan.for_dialect(D.TRINO),
]
