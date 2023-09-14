from __future__ import annotations

import sqlalchemy as sa

from bi_formula.core.datatype import DataType
from bi_formula.core.dialect import StandardDialect as D
from bi_formula.definitions.args import (
    ArgTypeForAll,
    ArgTypeSequence,
)
from bi_formula.definitions.base import (
    Function,
    TranslationVariant,
    TranslationVariantWrapped,
)
from bi_formula.definitions.common import make_binary_chain
from bi_formula.definitions.type_strategy import (
    Fixed,
    FromArgs,
    ParamsFromArgs,
)
from bi_formula.shortcuts import n

V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class MathFunction(Function):
    pass


class FuncAbs(MathFunction):
    name = "abs"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY | D.SQLITE, sa.func.ABS),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = FromArgs()


class FuncAcos(MathFunction):
    name = "acos"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.ACOS),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncAsin(MathFunction):
    name = "asin"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.ASIN),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncAtan(MathFunction):
    name = "atan"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.ATAN),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncAtan2(MathFunction):
    name = "atan2"
    arg_cnt = 2
    arg_names = ["x", "y"]
    variants = [
        V(D.DUMMY, sa.func.ATAN2),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncCeiling(MathFunction):
    name = "ceiling"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.CEIL),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncFloor(MathFunction):
    name = "floor"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.FLOOR),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncCos(MathFunction):
    name = "cos"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.COS),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncCot(MathFunction):
    name = "cot"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.COT),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncDegrees(MathFunction):
    name = "degrees"
    arg_cnt = 1
    arg_names = ["radians"]
    variants = [
        V(D.DUMMY, sa.func.DEGREES),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncDiv(MathFunction):
    name = "div"
    arg_cnt = 2
    arg_names = ["number_1", "number_2"]
    return_type = Fixed(DataType.INTEGER)


class FuncDivBasic(FuncDiv):
    variants = [
        V(D.DUMMY, sa.func.DIV),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]


class FuncDivSafeBase(MathFunction):
    name = "div_safe"
    return_type = Fixed(DataType.INTEGER)


class FuncDivSafe2(FuncDivSafeBase):
    arg_names = ["numerator", "denominator"]
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]
    variants = [
        V(D.DUMMY, lambda x, y: sa.func.div(x, sa.func.nullif(y, 0))),
    ]


class FuncDivSafe3(FuncDivSafeBase):
    arg_names = ["numerator", "denominator", "fallback_value"]
    arg_cnt = 3
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT, DataType.INTEGER]),
    ]
    variants = [V(D.DUMMY, lambda x, y, default: sa.func.coalesce(sa.func.div(x, sa.func.nullif(y, 0)), default))]


class FuncFDivSafeBase(MathFunction):
    name = "fdiv_safe"
    return_type = Fixed(DataType.FLOAT)


class FuncFDivSafe2(FuncFDivSafeBase):
    arg_names = ["numerator", "denominator"]
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]
    variants = [
        V(D.DUMMY, lambda x, y: x / sa.func.nullif(y, 0)),
    ]


class FuncFDivSafe3(FuncFDivSafeBase):
    arg_names = ["numerator", "denominator", "fallback_value"]
    arg_cnt = 3
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT, DataType.FLOAT]),
    ]
    variants = [V(D.DUMMY, lambda x, y, default: sa.func.coalesce(x / sa.func.nullif(y, 0), default))]


class FuncExp(MathFunction):
    name = "exp"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.EXP),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncLn(MathFunction):
    name = "ln"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.LN),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncLog10(MathFunction):
    name = "log10"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.LOG10),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncLog(MathFunction):
    name = "log"
    arg_cnt = 2
    arg_names = ["value", "base"]
    variants = [
        V(D.DUMMY, lambda x, base: sa.func.LOG(base, x)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


_COMPARABLE_TYPES = [
    DataType.INTEGER,
    DataType.FLOAT,
    DataType.BOOLEAN,
    DataType.STRING,
    DataType.DATE,
    DataType.DATETIME,
    DataType.DATETIMETZ,
    DataType.GENERICDATETIME,
]


class FuncGreatestBase(MathFunction):
    name = "greatest"
    arg_names = ["value_1", "value_2", "value_3"]
    arg_cnt = 2
    # TODO: sqlite3: just `max(a, b, ...)` works
    return_type = FromArgs()
    return_type_params = ParamsFromArgs(0)


class FuncGreatest1(FuncGreatestBase):
    arg_cnt = 1
    variants = [
        V(D.DUMMY | D.SQLITE, lambda value: value),
    ]
    argument_types = [
        ArgTypeSequence([set(_COMPARABLE_TYPES)]),
    ]


class FuncGreatestMain(FuncGreatestBase):
    variants = [V(D.DUMMY, sa.func.GREATEST)]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.DATE, DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME, DataType.DATETIME]),
        ArgTypeSequence([DataType.DATETIMETZ, DataType.DATETIMETZ]),
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
        ArgTypeSequence([DataType.BOOLEAN, DataType.BOOLEAN]),
    ]


class GreatestMulti(FuncGreatestBase):
    arg_cnt = None  # type: ignore  # TODO: fix
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda *args: make_binary_chain(
                # chain of GREATEST function calls
                (lambda x, y: n.func.GREATEST(x, y)),
                *args,
            ),
        ),
    ]
    argument_types = [
        # any type is allowed, but all arguments must be of the same type
        ArgTypeForAll({_t})
        for _t in _COMPARABLE_TYPES
    ]
    return_type_params = ParamsFromArgs(0)  # particularly for datetimetz


class FuncLeastBase(MathFunction):
    name = "least"
    arg_names = ["value_1", "value_2", "value_3"]
    arg_cnt = 2
    return_type = FromArgs()
    return_type_params = ParamsFromArgs(0)


class FuncLeast1(FuncLeastBase):
    arg_cnt = 1
    variants = [
        V(D.SQLITE, lambda value: value),
    ]
    argument_types = [
        ArgTypeSequence([set(_COMPARABLE_TYPES)]),
    ]


class FuncLeastMain(FuncLeastBase):
    variants = [
        V(D.DUMMY, sa.func.LEAST),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.DATE, DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME, DataType.DATETIME]),
        ArgTypeSequence([DataType.DATETIMETZ, DataType.DATETIMETZ]),
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
        ArgTypeSequence([DataType.BOOLEAN, DataType.BOOLEAN]),
    ]


class LeastMulti(FuncLeastBase):
    arg_cnt = None  # type: ignore  # TODO: fix
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda *args: make_binary_chain(
                # chain of LEAST function calls
                (lambda x, y: n.func.LEAST(x, y)),
                *args,
            ),
        ),
    ]
    argument_types = [
        # any type is allowed, but all arguments must be of the same type
        ArgTypeForAll({_t})
        for _t in _COMPARABLE_TYPES
    ]
    return_type_params = ParamsFromArgs(0)  # particularly for datetimetz


class FuncPi(MathFunction):
    name = "pi"
    arg_cnt = 0
    variants = [
        V(D.DUMMY, sa.func.PI),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncPower(MathFunction):
    name = "power"
    arg_cnt = 2
    arg_names = ["base", "power"]
    variants = [
        V(D.DUMMY, sa.func.POWER),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncRadians(MathFunction):
    name = "radians"
    arg_cnt = 1
    arg_names = ["degrees"]
    variants = [
        V(D.DUMMY, sa.func.RADIANS),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncRoundBase(MathFunction):
    name = "round"
    arg_names = ["number", "precision"]


class FuncRound1(FuncRoundBase):
    arg_cnt = 1
    variants = [
        V(D.DUMMY | D.SQLITE, sa.func.ROUND),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.INTEGER)


class FuncRound2(FuncRoundBase):
    arg_cnt = 2
    variants = [
        V(D.DUMMY | D.SQLITE, sa.func.ROUND),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.INTEGER]),
    ]
    return_type = FromArgs(0)


class FuncSign(MathFunction):
    name = "sign"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.SIGN),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.INTEGER)


class FuncSin(MathFunction):
    name = "sin"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.SIN),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncSqrt(MathFunction):
    name = "sqrt"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.SQRT),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncSquare(MathFunction):
    name = "square"
    arg_names = ["number"]
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = FromArgs()


class FuncTan(MathFunction):
    name = "tan"
    arg_names = ["number"]
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.TAN),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncCompare(MathFunction):
    name = "compare"
    arg_names = ["left", "right", "epsilon"]
    arg_cnt = 3
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT, DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.INTEGER)


DEFINITIONS_MATH = [
    # abs
    FuncAbs,
    # acos
    FuncAcos,
    # asin
    FuncAsin,
    # atan
    FuncAtan,
    # atan2
    FuncAtan2,
    # ceiling
    FuncCeiling,
    # compare
    FuncCompare,
    # cos
    FuncCos,
    # cot
    FuncCot,
    # degrees
    FuncDegrees,
    # div
    FuncDivBasic,
    # div_safe
    FuncDivSafe2,
    FuncDivSafe3,
    # fdiv_safe
    FuncFDivSafe2,
    FuncFDivSafe3,
    # exp
    FuncExp,
    # floor
    FuncFloor,
    # greatest
    FuncGreatest1,
    FuncGreatestMain,
    GreatestMulti,
    # least
    FuncLeast1,
    FuncLeastMain,
    LeastMulti,
    # ln
    FuncLn,
    # log
    FuncLog,
    # log10
    FuncLog10,
    # pi
    FuncPi,
    # power
    FuncPower,
    # radians
    FuncRadians,
    # round
    FuncRound1,
    FuncRound2,
    # sign
    FuncSign,
    # sin
    FuncSin,
    # sqrt
    FuncSqrt,
    # square
    FuncSquare,
    # tan
    FuncTan,
]
