import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common_datetime import DAY_USEC
import dl_formula.definitions.operators_binary as base

from bi_connector_yql.formula.constants import YqlDialect as D


V = TranslationVariant.make


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.YQL),
    # %
    base.BinaryModInteger.for_dialect(D.YQL),
    base.BinaryModFloat.for_dialect(D.YQL),
    # *
    base.BinaryMultNumbers.for_dialect(D.YQL),
    base.BinaryMultStringConst.for_dialect(D.YQL),
    # +
    base.BinaryPlusNumbers.for_dialect(D.YQL),
    base.BinaryPlusStrings.for_dialect(D.YQL),
    base.BinaryPlusDateInt(
        variants=[
            V(D.YQL, lambda date, days: date + sa.func.DateTime.IntervalFromDays(days)),
        ]
    ),
    base.BinaryPlusDateFloat(
        variants=[
            V(D.YQL, lambda date, days: date + sa.func.DateTime.IntervalFromDays(sa.cast(days, sa.INTEGER))),
        ]
    ),
    base.BinaryPlusDatetimeNumber(
        variants=[
            V(
                D.YQL,
                lambda date, days: (date + sa.func.DateTime.IntervalFromMicroseconds(base.as_bigint(days * DAY_USEC))),
            ),
        ]
    ),
    base.BinaryPlusGenericDatetimeNumber(
        variants=[
            V(
                D.YQL,
                lambda dt, days: (dt + sa.func.DateTime.IntervalFromMicroseconds(base.as_bigint(days * DAY_USEC))),
            ),
        ]
    ),
    # -
    base.BinaryMinusInts(
        variants=[
            V(D.YQL, lambda num1, num2: (sa.cast(num1, sa.INTEGER) - sa.cast(num2, sa.INTEGER))),
        ]
    ),
    base.BinaryMinusNumbers.for_dialect(D.YQL),
    base.BinaryMinusDateInt(
        variants=[
            V(D.YQL, lambda date, days: date - sa.func.DateTime.IntervalFromDays(days)),
        ]
    ),
    base.BinaryMinusDateFloat(
        variants=[
            V(
                D.YQL,
                lambda date, days: (
                    date - sa.func.DateTime.IntervalFromDays(sa.cast(sa.func.Math.Ceil(days), sa.INTEGER))
                ),
            ),
        ]
    ),
    base.BinaryMinusDatetimeNumber(
        variants=[
            V(
                D.YQL,
                lambda date, days: (date - sa.func.DateTime.IntervalFromMicroseconds(base.as_bigint(days * DAY_USEC))),
            ),
        ]
    ),
    base.BinaryMinusGenericDatetimeNumber(
        variants=[
            V(
                D.YQL,
                lambda dt, days: (dt - sa.func.DateTime.IntervalFromMicroseconds(base.as_bigint(days * DAY_USEC))),
            ),
        ]
    ),
    base.BinaryMinusDates(
        variants=[
            V(D.YQL, lambda left, right: sa.func.DateTime.ToDays(left - right)),
        ]
    ),
    base.BinaryMinusDatetimes(
        variants=[
            V(D.YQL, lambda left, right: sa.func.DateTime.ToMicroseconds(left - right) / float(DAY_USEC)),
        ]
    ),
    base.BinaryMinusGenericDatetimes(
        variants=[
            V(D.YQL, lambda left, right: sa.func.DateTime.ToMicroseconds(left - right) / float(DAY_USEC)),
        ]
    ),
    # /
    base.BinaryDivInt(
        variants=[
            # See also: https://yql.yandex-team.ru/docs/ydb/?singlePage=true#_syntax_pragma_classicdivision
            V(D.YQL, lambda x, y: sa.cast(x, sa.FLOAT) / y),
        ]
    ),
    base.BinaryDivFloat.for_dialect(D.YQL),
    # <
    base.BinaryLessThan.for_dialect(D.YQL),
    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.YQL),
    # ==
    base.BinaryEqual.for_dialect(D.YQL),
    # >
    base.BinaryGreaterThan.for_dialect(D.YQL),
    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.YQL),
    # ^
    base.BinaryPower.for_dialect(D.YQL),
    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.YQL),
    # _==
    base.BinaryEqualInternal.for_dialect(D.YQL),
    # _dneq
    base.BinaryEqualDenullified(
        variants=[
            # YQL does not support ISNULL and other complex operations in JOIN conditions
            V(D.YQL, lambda left, right: left == right),  # type: ignore
        ]
    ),
    # and
    base.BinaryAnd.for_dialect(D.YQL),
    # in
    base.BinaryIn.for_dialect(D.YQL),
    # like
    base.BinaryLike.for_dialect(D.YQL),
    # notin
    base.BinaryNotIn.for_dialect(D.YQL),
    # notlike
    base.BinaryNotLike.for_dialect(D.YQL),
    # or
    base.BinaryOr.for_dialect(D.YQL),
]
