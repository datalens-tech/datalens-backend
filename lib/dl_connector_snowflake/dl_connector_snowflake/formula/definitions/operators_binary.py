import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common_datetime import DAY_SEC
import dl_formula.definitions.operators_binary as base

from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D

V = TranslationVariant.make


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.SNOWFLAKE),
    # %
    base.BinaryModInteger(
        variants=[
            V(D.SNOWFLAKE, sa.func.MOD),
        ]
    ),
    base.BinaryModFloat(
        variants=[
            V(D.SNOWFLAKE, lambda left, right: left - sa.func.FLOOR(left / right) * right),
        ]
    ),
    # *
    base.BinaryMultNumbers.for_dialect(D.SNOWFLAKE),
    base.BinaryMultStringConst.for_dialect(D.SNOWFLAKE),
    base.BinaryMultStringNonConst(
        variants=[
            V(D.SNOWFLAKE, sa.func.REPEAT),
        ]
    ),
    # +
    base.BinaryPlusNumbers.for_dialect(D.SNOWFLAKE),
    base.BinaryPlusStrings.for_dialect(D.SNOWFLAKE),
    base.BinaryPlusDateInt(
        variants=[
            V(D.SNOWFLAKE, lambda date, days: date + days),
        ]
    ),
    base.BinaryPlusDateFloat(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda date, days: sa.func.TO_DATE(sa.func.DATEADD("second", DAY_SEC * days, date)),
            )
        ]
    ),
    base.BinaryPlusDatetimeNumber(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda date, days: sa.func.DATEADD("second", DAY_SEC * days, date),
            ),
        ]
    ),
    base.BinaryPlusGenericDatetimeNumber(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda date, days: sa.func.DATEADD("second", DAY_SEC * days, date),
            ),
        ]
    ),
    # -
    base.BinaryMinusNumbers.for_dialect(D.SNOWFLAKE),
    base.BinaryMinusDateInt(
        variants=[
            V(D.SNOWFLAKE, lambda date, days: date - days),
        ]
    ),
    base.BinaryMinusDateFloat(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda date, days: sa.func.TO_DATE(sa.func.DATEADD("second", -DAY_SEC * days, date)),
            ),
        ]
    ),
    base.BinaryMinusDatetimeNumber(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda date, days: sa.func.DATEADD("second", -DAY_SEC * days, date),
            ),
        ]
    ),
    base.BinaryMinusGenericDatetimeNumber(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda date, days: sa.func.DATEADD("second", -DAY_SEC * days, date),
            ),
        ]
    ),
    base.BinaryMinusDates(
        variants=[
            V(D.SNOWFLAKE, lambda left, right: sa.func.DATEDIFF("second", right, left) / DAY_SEC),
        ]
    ),
    base.BinaryMinusDatetimes(
        variants=[
            V(D.SNOWFLAKE, lambda left, right: sa.func.DATEDIFF("second", right, left) / DAY_SEC),
        ]
    ),
    base.BinaryMinusGenericDatetimes(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda left, right: sa.func.DATEDIFF("second", right, left) / DAY_SEC,
            ),
        ]
    ),
    # /
    base.BinaryDivInt.for_dialect(D.SNOWFLAKE),
    base.BinaryDivFloat.for_dialect(D.SNOWFLAKE),
    # <
    base.BinaryLessThan.for_dialect(D.SNOWFLAKE),
    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.SNOWFLAKE),
    # ==
    base.BinaryEqual.for_dialect(D.SNOWFLAKE),
    # >
    base.BinaryGreaterThan.for_dialect(D.SNOWFLAKE),
    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.SNOWFLAKE),
    # ^
    base.BinaryPower.for_dialect(D.SNOWFLAKE),
    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.SNOWFLAKE),
    # _==
    base.BinaryEqualInternal.for_dialect(D.SNOWFLAKE),
    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.SNOWFLAKE),
    # and
    base.BinaryAnd.for_dialect(D.SNOWFLAKE),
    # in
    base.BinaryIn.for_dialect(D.SNOWFLAKE),
    # like
    base.BinaryLike.for_dialect(D.SNOWFLAKE),
    # notin
    base.BinaryNotIn.for_dialect(D.SNOWFLAKE),
    # notlike
    base.BinaryNotLike.for_dialect(D.SNOWFLAKE),
    # or
    base.BinaryOr.for_dialect(D.SNOWFLAKE),
]
