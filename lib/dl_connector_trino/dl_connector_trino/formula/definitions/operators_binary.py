from typing import Callable

import sqlalchemy as sa
import sqlalchemy.sql.expression as sa_expr
import trino.sqlalchemy.datatype as tsa

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import TransCallResult
from dl_formula.definitions.common_datetime import DAY_SEC
import dl_formula.definitions.operators_binary as base

from dl_connector_trino.formula.constants import TrinoDialect as D


def generic_datetime_plus_number_factory(operator: str) -> Callable:
    assert operator in ("+", "-")

    def _func(datetime: sa.sql.ColumnElement, days: sa.sql.ColumnElement) -> TransCallResult:
        signed_days = days if operator == "+" else -days

        if isinstance(days.type, sa.Integer):
            return sa.func.date_add("day", signed_days, datetime)

        milliseconds = signed_days * DAY_SEC * 1000
        int_milliseconds = sa.cast(sa.func.floor(milliseconds), sa.BIGINT)
        return sa.func.date_add("millisecond", int_milliseconds, datetime)

    return _func


def generic_datetime_diff(left: sa.sql.ColumnElement, right: sa.sql.ColumnElement) -> TransCallResult:
    milliseconds = sa.func.date_diff("millisecond", right, left)
    return sa.cast(milliseconds, tsa.DOUBLE) / (DAY_SEC * 1000)


V = TranslationVariant.make

DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.TRINO),
    # %
    base.BinaryModInteger(
        variants=[
            V(D.TRINO, sa.func.mod),
        ]
    ),
    base.BinaryModFloat(
        variants=[
            V(D.TRINO, sa.func.mod),
        ]
    ),
    # *
    base.BinaryMultNumbers.for_dialect(D.TRINO),
    base.BinaryMultStringConst.for_dialect(D.TRINO),
    base.BinaryMultStringNonConst(
        variants=[
            V(D.TRINO, lambda string, num: sa.func.concat_ws("", sa.func.repeat(string, num))),
        ]
    ),
    # +
    base.BinaryPlusNumbers.for_dialect(D.TRINO),
    base.BinaryPlusStrings(
        variants=[
            V(D.TRINO, sa.func.concat),
        ]
    ),
    base.BinaryPlusArray(
        variants=[
            V(D.TRINO, lambda left, right: sa_expr.BinaryExpression(left, right, sa_expr.custom_op("||"))),
        ]
    ),
    base.BinaryPlusDateInt(
        variants=[
            V(D.TRINO, lambda date, days: sa.func.date_add("day", days, date)),
        ]
    ),
    base.BinaryPlusDateFloat(
        variants=[
            V(D.TRINO, lambda date, days: sa.func.date_add("day", sa.cast(days, sa.Integer), date)),
        ]
    ),
    # base.BinaryPlusDatetimeNumber.for_dialect(D.TRINO),
    base.BinaryPlusGenericDatetimeNumber(
        variants=[
            V(D.TRINO, generic_datetime_plus_number_factory("+")),
        ]
    ),
    # -
    base.BinaryMinusNumbers.for_dialect(D.TRINO),
    base.BinaryMinusDateInt(
        variants=[
            V(D.TRINO, lambda date, days: sa.func.date_add("day", -days, date)),
        ]
    ),
    base.BinaryMinusDateFloat(
        variants=[
            V(D.TRINO, lambda date, days: sa.func.date_add("day", sa.cast(-sa.func.ceil(days), sa.BIGINT), date)),
        ]
    ),
    # base.BinaryMinusDatetimeNumber.for_dialect(D.TRINO),
    base.BinaryMinusGenericDatetimeNumber(
        variants=[
            V(D.TRINO, generic_datetime_plus_number_factory("-")),
        ]
    ),
    base.BinaryMinusDates(
        variants=[
            V(D.TRINO, lambda left, right: sa.func.date_diff("day", right, left)),
        ]
    ),
    # # base.BinaryMinusDatetimes.for_dialect(D.TRINO),
    base.BinaryMinusGenericDatetimes(
        variants=[
            V(D.TRINO, generic_datetime_diff),
        ]
    ),
    # /
    base.BinaryDivInt(
        variants=[
            V(D.TRINO, lambda left, right: sa.cast(left, tsa.DOUBLE) / right),
        ]
    ),
    base.BinaryDivFloat.for_dialect(D.TRINO),
    # <
    base.BinaryLessThan.for_dialect(D.TRINO),
    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.TRINO),
    # ==
    base.BinaryEqual.for_dialect(D.TRINO),
    # >
    base.BinaryGreaterThan.for_dialect(D.TRINO),
    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.TRINO),
    # ^
    base.BinaryPower(
        variants=[
            V(D.TRINO, sa.func.power),
        ]
    ),
    # _!=
    # base.BinaryNotEqualInternal.for_dialect(D.TRINO),
    # _==
    # base.BinaryEqualInternal.for_dialect(D.TRINO),
    # _dneq
    # base.BinaryEqualDenullified.for_dialect(D.TRINO),
    # and
    base.BinaryAnd.for_dialect(D.TRINO),
    # in
    base.BinaryIn.for_dialect(D.TRINO),
    # like
    base.BinaryLike.for_dialect(D.TRINO),
    # notin
    base.BinaryNotIn.for_dialect(D.TRINO),
    # notlike
    base.BinaryNotLike.for_dialect(D.TRINO),
    # or
    # base.BinaryOr.for_dialect(D.TRINO),
]
