import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import raw_sql
from dl_formula.definitions.common_datetime import (
    DAY_SEC,
    DAY_USEC,
)
import dl_formula.definitions.operators_binary as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.STARROCKS),
    # %
    base.BinaryModInteger.for_dialect(D.STARROCKS),
    base.BinaryModFloat.for_dialect(D.STARROCKS),
    # *
    base.BinaryMultNumbers.for_dialect(D.STARROCKS),
    base.BinaryMultStringConst.for_dialect(D.STARROCKS),
    base.BinaryMultStringNonConst(
        variants=[
            V(D.STARROCKS, sa.func.REPEAT),
        ]
    ),
    # +
    base.BinaryPlusNumbers.for_dialect(D.STARROCKS),
    base.BinaryPlusStrings(
        variants=[
            V(D.STARROCKS, sa.func.CONCAT),
        ]
    ),
    base.BinaryPlusDateInt(
        variants=[
            V(D.STARROCKS, lambda date, days: sa.func.FROM_DAYS(sa.func.TO_DAYS(date) + days)),
        ]
    ),
    base.BinaryPlusDateFloat(
        variants=[
            V(D.STARROCKS, lambda date, days: sa.func.FROM_DAYS(sa.func.TO_DAYS(date) + base.as_bigint(days))),
        ]
    ),
    base.BinaryPlusDatetimeNumber(
        variants=[
            V(D.STARROCKS, lambda dt, days: sa.func.FROM_UNIXTIME(sa.func.UNIX_TIMESTAMP(dt) + days * DAY_SEC)),
        ]
    ),
    base.BinaryPlusGenericDatetimeNumber(
        variants=[
            V(D.STARROCKS, lambda dt, days: sa.func.FROM_UNIXTIME(sa.func.UNIX_TIMESTAMP(dt) + days * DAY_SEC)),
        ]
    ),
    # -
    base.BinaryMinusNumbers.for_dialect(D.STARROCKS),
    base.BinaryMinusDateInt(
        variants=[
            V(D.STARROCKS, lambda date, days: sa.func.FROM_DAYS(sa.func.TO_DAYS(date) - days)),
        ]
    ),
    base.BinaryMinusDateFloat(
        variants=[
            V(
                D.STARROCKS,
                lambda date, days: sa.func.FROM_DAYS(sa.func.TO_DAYS(date) - base.as_bigint(sa.func.CEIL(days))),
            ),
        ]
    ),
    base.BinaryMinusDatetimeNumber(
        variants=[
            V(D.STARROCKS, lambda dt, days: sa.func.FROM_UNIXTIME(sa.func.UNIX_TIMESTAMP(dt) - days * DAY_SEC)),
        ]
    ),
    base.BinaryMinusGenericDatetimeNumber(
        variants=[
            V(D.STARROCKS, lambda dt, days: sa.func.FROM_UNIXTIME(sa.func.UNIX_TIMESTAMP(dt) - days * DAY_SEC)),
        ]
    ),
    base.BinaryMinusDates(
        variants=[
            V(D.STARROCKS, sa.func.DATEDIFF),
        ]
    ),
    base.BinaryMinusDatetimes(
        variants=[
            V(
                D.STARROCKS,
                lambda left, right: (sa.func.UNIX_TIMESTAMP(left) - sa.func.UNIX_TIMESTAMP(right)) / DAY_SEC,
            ),
        ]
    ),
    base.BinaryMinusGenericDatetimes(
        variants=[
            V(D.STARROCKS, lambda left, right: sa.func.TIMESTAMPDIFF(raw_sql("second"), right, left) / DAY_SEC),
        ]
    ),
    # /
    base.BinaryDivInt.for_dialect(D.STARROCKS),
    base.BinaryDivFloat.for_dialect(D.STARROCKS),
    # <
    base.BinaryLessThan.for_dialect(D.STARROCKS),
    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.STARROCKS),
    # ==
    base.BinaryEqual.for_dialect(D.STARROCKS),
    # >
    base.BinaryGreaterThan.for_dialect(D.STARROCKS),
    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.STARROCKS),
    # ^
    base.BinaryPower.for_dialect(D.STARROCKS),
    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.STARROCKS),
    # _==
    base.BinaryEqualInternal.for_dialect(D.STARROCKS),
    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.STARROCKS),
    # and
    base.BinaryAnd.for_dialect(D.STARROCKS),
    # in
    base.BinaryIn.for_dialect(D.STARROCKS),
    # like
    base.BinaryLike.for_dialect(D.STARROCKS),
    # notin
    base.BinaryNotIn.for_dialect(D.STARROCKS),
    # notlike
    base.BinaryNotLike.for_dialect(D.STARROCKS),
    # or
    base.BinaryOr.for_dialect(D.STARROCKS),
]
