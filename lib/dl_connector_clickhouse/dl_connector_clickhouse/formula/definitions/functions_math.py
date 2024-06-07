import sqlalchemy as sa

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import dl_formula.definitions.functions_math as base
from dl_formula.shortcuts import n

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class FuncGreatestCH(base.FuncGreatestBase):
    variants = [V(D.CLICKHOUSE, lambda x, y: getattr(sa.func, "if")(x >= y, x, y))]
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME, DataType.DATETIME]),
        ArgTypeSequence([DataType.DATETIMETZ, DataType.DATETIMETZ]),
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
        ArgTypeSequence([DataType.BOOLEAN, DataType.BOOLEAN]),
    ]


class FuncGreatestNumbersCH(base.FuncGreatestBase):
    variants = [
        V(D.CLICKHOUSE, sa.func.greatest),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]


class FuncLeastCH(base.FuncLeastBase):
    variants = [
        V(D.CLICKHOUSE, sa.func.least),
    ]
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME, DataType.DATETIME]),
        ArgTypeSequence([DataType.DATETIMETZ, DataType.DATETIMETZ]),
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
        ArgTypeSequence([DataType.BOOLEAN, DataType.BOOLEAN]),
    ]


class FuncLeastNumbersCH(base.FuncLeastBase):
    variants = [
        V(D.CLICKHOUSE, sa.func.least),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]


DEFINITIONS_MATH = [
    # abs
    base.FuncAbs.for_dialect(D.CLICKHOUSE),
    # acos
    base.FuncAcos.for_dialect(D.CLICKHOUSE),
    # asin
    base.FuncAsin.for_dialect(D.CLICKHOUSE),
    # atan
    base.FuncAtan.for_dialect(D.CLICKHOUSE),
    # atan2
    base.FuncAtan2(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda x, y: n.if_(
                    n.if_(sa.and_(x == 0, y == 0)).then(0),  # type: ignore  # TODO: fix
                    n.if_(sa.and_(y == 0, x < 0)).then(-sa.func.pi() / 2),  # type: ignore  # TODO: fix
                    n.if_(sa.and_(y == 0, x > 0)).then(sa.func.pi() / 2),  # type: ignore  # TODO: fix
                    n.if_(sa.and_(x == 0, y < 0)).then(sa.func.pi()),  # type: ignore  # TODO: fix
                    n.if_(sa.and_(x == 0, y > 0)).then(0),  # type: ignore  # TODO: fix
                    n.if_(y > 0).then(sa.func.atan(x / y)),  # type: ignore  # TODO: fix
                    n.if_(x > 0).then(sa.func.atan(x / y) + sa.func.pi()),  # type: ignore  # TODO: fix
                ).else_(
                    sa.func.atan(x / y) - sa.func.pi()  # for x < 0
                ),
            ),
        ]
    ),
    # ceiling
    base.FuncCeiling(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda num: sa.func.ceil(num) + 0,
            )
        ]
    ),
    # compare
    base.FuncCompare(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda left, right, epsilon: sa.func.multiIf(
                    sa.func.lessOrEquals(sa.func.abs(left - right), epsilon),
                    0,
                    sa.func.less(left, right),
                    -1,
                    1,
                ),
            ),
        ]
    ),
    # cos
    base.FuncCos.for_dialect(D.CLICKHOUSE),
    # cot
    base.FuncCot(
        variants=[
            V(D.CLICKHOUSE, lambda x: sa.func.cos(x) / sa.func.sin(x)),
        ]
    ),
    # degrees
    base.FuncDegrees(
        variants=[
            V(D.CLICKHOUSE, lambda x: x / sa.func.pi() * 180),
        ]
    ),
    # div
    base.FuncDivBasic(
        variants=[
            V(D.CLICKHOUSE, sa.func.intDiv),
        ]
    ),
    # div_safe
    base.FuncDivSafe2(
        variants=[
            V(D.CLICKHOUSE, lambda x, y: sa.func.IF(y != 0, sa.func.intDivOrZero(x, y), None)),
        ]
    ),
    base.FuncDivSafe3(
        variants=[
            V(D.CLICKHOUSE, lambda x, y, default: sa.func.IF(y != 0, sa.func.intDivOrZero(x, y), default)),
        ]
    ),
    # exp
    base.FuncExp.for_dialect(D.CLICKHOUSE),
    # fdiv_safe
    base.FuncFDivSafe2.for_dialect(D.CLICKHOUSE),
    base.FuncFDivSafe3.for_dialect(D.CLICKHOUSE),
    # floor
    base.FuncFloor.for_dialect(D.CLICKHOUSE),
    # greatest
    base.FuncGreatest1.for_dialect(D.CLICKHOUSE),
    FuncGreatestCH(),
    FuncGreatestNumbersCH(),
    base.GreatestMulti.for_dialect(D.CLICKHOUSE),
    # least
    base.FuncLeast1.for_dialect(D.CLICKHOUSE),
    FuncLeastCH(),
    FuncLeastNumbersCH(),
    base.LeastMulti.for_dialect(D.CLICKHOUSE),
    # ln
    base.FuncLn(
        variants=[
            V(D.CLICKHOUSE, sa.func.log),
        ]
    ),
    # log
    base.FuncLog(
        variants=[
            V(D.CLICKHOUSE, lambda x, y: sa.func.log(x) / sa.func.log(y)),
        ]
    ),
    # log10
    base.FuncLog10.for_dialect(D.CLICKHOUSE),
    # pi
    base.FuncPi.for_dialect(D.CLICKHOUSE),
    # power
    base.FuncPower(
        variants=[
            V(D.CLICKHOUSE, sa.func.pow),
        ]
    ),
    # radians
    base.FuncRadians(
        variants=[
            V(D.CLICKHOUSE, lambda x: x * sa.func.pi() / 180),
        ]
    ),
    # round
    base.FuncRound1(
        variants=[
            V(D.CLICKHOUSE, lambda num: sa.func.round(num) + 0),
        ],
    ),
    base.FuncRound2(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda num, precision: sa.func.round(num, precision) + 0,
            )
        ]
    ),
    # sign
    base.FuncSign(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda x: n.if_(
                    n.if_(x < 0).then(-1),  # type: ignore  # TODO: fix
                    n.if_(x > 0).then(1),  # type: ignore  # TODO: fix
                ).else_(0),
            ),
        ]
    ),
    # sin
    base.FuncSin.for_dialect(D.CLICKHOUSE),
    # sqrt
    base.FuncSqrt.for_dialect(D.CLICKHOUSE),
    # square
    base.FuncSquare(
        variants=[
            V(D.CLICKHOUSE, lambda x: sa.func.pow(x, 2)),
        ]
    ),
    # tan
    base.FuncTan.for_dialect(D.CLICKHOUSE),
]
