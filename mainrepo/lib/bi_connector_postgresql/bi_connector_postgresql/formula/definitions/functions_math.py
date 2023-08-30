from typing import ClassVar

import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from bi_connector_postgresql.formula.constants import PostgreSQLDialect as D
from bi_formula.core.datatype import DataType
import bi_formula.definitions.functions_math as base
from bi_formula.definitions.args import ArgTypeSequence
from bi_formula.definitions.base import TranslationVariant
from bi_formula.definitions.literals import un_literal, Literal


V = TranslationVariant.make


class FuncRound2ConstCompeng(base.FuncRound2):
    _DECIMAL_SIZE: ClassVar[int] = 20

    variants = [
        V(
            D.COMPENG,
            lambda number, precision: FuncRound2ConstCompeng._do_round_with_const_arg(number, precision),
        ),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.CONST_INTEGER]),
    ]

    @classmethod
    def _do_round_with_const_arg(self, number: Literal, precision: Literal) -> ClauseElement:
        precision_value = un_literal(precision)
        dec_precision = self._DECIMAL_SIZE
        dec_scale = precision_value+5
        return sa.func.ROUND(
            sa.cast(number, sa.Numeric(precision=dec_precision, scale=dec_scale)),
            precision
        )


DEFINITIONS_MATH = [
    # abs
    base.FuncAbs.for_dialect(D.POSTGRESQL),

    # acos
    base.FuncAcos.for_dialect(D.POSTGRESQL),

    # asin
    base.FuncAsin.for_dialect(D.POSTGRESQL),

    # atan
    base.FuncAtan.for_dialect(D.POSTGRESQL),

    # atan2
    base.FuncAtan2.for_dialect(D.POSTGRESQL),

    # ceiling
    base.FuncCeiling.for_dialect(D.POSTGRESQL),

    # cos
    base.FuncCos.for_dialect(D.POSTGRESQL),

    # cot
    base.FuncCot.for_dialect(D.POSTGRESQL),

    # degrees
    base.FuncDegrees.for_dialect(D.POSTGRESQL),

    # div
    base.FuncDivBasic(variants=[
        V(D.COMPENG, lambda x, y: sa.func.div(x, sa.func.nullif(y, 0))),
    ]),
    base.FuncDivBasic.for_dialect(D.NON_COMPENG_POSTGRESQL),

    # div_safe
    base.FuncDivSafe2.for_dialect(D.POSTGRESQL),
    base.FuncDivSafe3.for_dialect(D.POSTGRESQL),

    # exp
    base.FuncExp.for_dialect(D.POSTGRESQL),

    # fdiv_safe
    base.FuncFDivSafe2.for_dialect(D.POSTGRESQL),
    base.FuncFDivSafe3.for_dialect(D.POSTGRESQL),

    # floor
    base.FuncFloor.for_dialect(D.POSTGRESQL),

    # greatest
    base.FuncGreatest1.for_dialect(D.POSTGRESQL),
    base.FuncGreatestMain.for_dialect(D.POSTGRESQL),
    base.GreatestMulti.for_dialect(D.POSTGRESQL),

    # least
    base.FuncLeast1.for_dialect(D.POSTGRESQL),
    base.FuncLeastMain.for_dialect(D.POSTGRESQL),
    base.LeastMulti.for_dialect(D.POSTGRESQL),

    # ln
    base.FuncLn.for_dialect(D.POSTGRESQL),

    # log
    base.FuncLog(variants=[
        V(
            D.COMPENG,
            lambda x, base: (
                sa.func.ln(sa.func.nullif(x, 0)) /
                sa.func.nullif(sa.func.ln(sa.func.nullif(base, 0)), 0)
            )
        ),
    ]),
    base.FuncLog(variants=[
        V(D.NON_COMPENG_POSTGRESQL, lambda x, base: sa.func.ln(x) / sa.func.ln(base)),
    ]),

    # log10
    base.FuncLog10(variants=[
        V(D.POSTGRESQL, sa.func.LOG),
    ]),

    # pi
    base.FuncPi.for_dialect(D.POSTGRESQL),

    # power
    base.FuncPower.for_dialect(D.POSTGRESQL),

    # radians
    base.FuncRadians.for_dialect(D.POSTGRESQL),

    # round
    FuncRound2ConstCompeng(),  # To avoid manual casts to numeric on user's side in COMPENG
    base.FuncRound1.for_dialect(D.POSTGRESQL),
    base.FuncRound2.for_dialect(D.POSTGRESQL),

    # sign
    base.FuncSign.for_dialect(D.POSTGRESQL),

    # sin
    base.FuncSin.for_dialect(D.POSTGRESQL),

    # sqrt
    base.FuncSqrt.for_dialect(D.POSTGRESQL),

    # square
    base.FuncSquare(variants=[
        V(D.POSTGRESQL, lambda x: sa.func.POW(x, 2)),
    ]),

    # tan
    base.FuncTan.for_dialect(D.POSTGRESQL),
]
