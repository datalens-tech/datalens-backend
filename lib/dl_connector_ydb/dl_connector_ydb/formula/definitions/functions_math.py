import sqlalchemy as sa

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import dl_formula.definitions.functions_math as base
from dl_formula.shortcuts import n

from dl_connector_ydb.formula.constants import YqlDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


DEFINITIONS_MATH = [
    # abs
    base.FuncAbs(
        variants=[
            V(D.YQL, sa.func.Math.Abs),  # `Math::Abs(…)`
        ]
    ),
    # acos
    base.FuncAcos(
        variants=[
            V(D.YQL, sa.func.Math.Acos),
        ]
    ),
    # asin
    base.FuncAsin(
        variants=[
            V(D.YQL, sa.func.Math.Asin),
        ]
    ),
    # atan
    base.FuncAtan(
        variants=[
            V(D.YQL, sa.func.Math.Atan),
        ]
    ),
    # atan2
    base.FuncAtan2(
        variants=[
            V(D.YQL, sa.func.Math.Atan2),
        ]
    ),
    # ceiling
    base.FuncCeiling(
        variants=[
            V(D.YQL, lambda num: sa.func.Math.Ceil(num) + 0),
        ]
    ),
    # cos
    base.FuncCos(
        variants=[
            V(D.YQL, sa.func.Math.Cos),
        ]
    ),
    # cot
    base.FuncCot(
        variants=[
            V(D.YQL, lambda x: sa.func.Math.Cos(x) / sa.func.Math.Sin(x)),
        ]
    ),
    # degrees
    base.FuncDegrees(
        variants=[
            V(D.YQL, lambda x: x / sa.func.Math.Pi() * 180.0),
        ]
    ),
    # div
    base.FuncDivBasic(
        variants=[
            V(D.YQL, lambda x, y: sa.cast(x / y, sa.BIGINT)),
        ]
    ),
    # div_safe
    base.FuncDivSafe2(
        variants=[
            V(D.YQL, lambda x, y: sa.cast(sa.func.IF(y != 0, x / y), sa.BIGINT)),
        ]
    ),
    base.FuncDivSafe3(
        variants=[
            V(D.YQL, lambda x, y, default: sa.cast(sa.func.IF(y != 0, x / y, default), sa.BIGINT)),
        ]
    ),
    # exp
    base.FuncExp(
        variants=[
            V(D.YQL, sa.func.Math.Exp),
        ]
    ),
    # fdiv_safe
    base.FuncFDivSafe2(
        variants=[
            V(D.YQL, lambda x, y: sa.func.IF(y != 0, x / y)),
        ]
    ),
    base.FuncFDivSafe3(
        variants=[
            V(D.YQL, lambda x, y, default: sa.func.IF(y != 0, x / y, default)),
        ]
    ),
    # floor
    base.FuncFloor(
        variants=[
            V(D.YQL, sa.func.Math.Floor),
        ]
    ),
    # greatest
    base.FuncGreatest1.for_dialect(D.YQL),
    base.FuncGreatestMain.for_dialect(D.YQL),
    base.GreatestMulti.for_dialect(D.YQL),
    # least
    base.FuncLeast1.for_dialect(D.YQL),
    base.FuncLeastMain.for_dialect(D.YQL),
    base.LeastMulti.for_dialect(D.YQL),
    # ln
    base.FuncLn(
        variants=[
            V(D.YQL, sa.func.Math.Log),
        ]
    ),
    # log
    base.FuncLog(
        variants=[
            V(D.YQL, lambda x, y: sa.func.Math.Log(x) / sa.func.Math.Log(y)),
        ]
    ),
    # log10
    base.FuncLog10(
        variants=[
            V(D.YQL, sa.func.Math.Log10),
        ]
    ),
    # pi
    base.FuncPi(
        variants=[
            V(D.YQL, sa.func.Math.Pi),
        ]
    ),
    # power
    base.FuncPower(
        variants=[
            V(D.YQL, sa.func.Math.Pow),
        ]
    ),
    # radians
    base.FuncRadians(
        variants=[
            V(D.YQL, lambda x: x * sa.func.Math.Pi() / 180.0),
        ]
    ),
    # round
    base.FuncRound1(
        variants=[
            V(D.YQL, sa.func.Math.Round),
        ]
    ),
    base.FuncRound2(
        variants=[
            # in YQL Math::Round takes power of 10 instead of precision, so we have to invert the `num` value
            V(D.YQL, lambda x, num: sa.func.Math.Round(x, -num)),
        ]
    ),
    # sign
    base.FuncSign(
        variants=[
            V(
                D.YQL,
                lambda x: n.if_(
                    n.if_(x < 0).then(-1),  # type: ignore  # TODO: fix
                    n.if_(x > 0).then(1),  # type: ignore  # TODO: fix
                ).else_(0),
            ),
        ]
    ),
    # sin
    base.FuncSin(
        variants=[
            V(D.YQL, sa.func.Math.Sin),
        ]
    ),
    # sqrt
    base.FuncSqrt(
        variants=[
            V(D.YQL, sa.func.Math.Sqrt),
        ]
    ),
    # square
    base.FuncSquare(
        variants=[
            V(D.YQL, lambda x: sa.func.Math.Pow(x, 2)),
        ]
    ),
    # tan
    base.FuncTan(
        variants=[
            V(D.YQL, sa.func.Math.Tan),
        ]
    ),
]
