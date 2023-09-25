import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_postgresql

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D
from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common_datetime import DAY_SEC
import dl_formula.definitions.operators_binary as base


V = TranslationVariant.make


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.POSTGRESQL),
    # %
    base.BinaryModInteger.for_dialect(D.POSTGRESQL),
    base.BinaryModFloat.for_dialect(D.POSTGRESQL),
    # *
    base.BinaryMultNumbers.for_dialect(D.POSTGRESQL),
    base.BinaryMultStringConst.for_dialect(D.POSTGRESQL),
    base.BinaryMultStringNonConst(
        variants=[
            V(D.POSTGRESQL, sa.func.REPEAT),
        ]
    ),
    # +
    base.BinaryPlusNumbers.for_dialect(D.POSTGRESQL),
    base.BinaryPlusStrings.for_dialect(D.POSTGRESQL),
    base.BinaryPlusDateInt(
        variants=[
            V(
                D.POSTGRESQL,
                lambda date, days: sa.cast(
                    sa.func.to_timestamp(sa.func.extract("epoch", date) + days * DAY_SEC), sa.Date
                ),
            ),
        ]
    ),
    base.BinaryPlusArray(
        variants=[
            V(D.POSTGRESQL, sa.func.array_cat),
        ]
    ),
    base.BinaryPlusDateFloat(
        variants=[
            V(
                D.POSTGRESQL,
                lambda date, days: sa.cast(
                    sa.func.to_timestamp(sa.func.extract("epoch", date) + base.as_bigint(days) * DAY_SEC), sa.Date
                ),
            ),
        ]
    ),
    base.BinaryPlusDatetimeNumber(
        variants=[
            V(D.POSTGRESQL, lambda dt, days: sa.func.to_timestamp(sa.func.extract("epoch", dt) + days * DAY_SEC)),
        ]
    ),
    base.BinaryPlusGenericDatetimeNumber(
        variants=[
            V(
                D.POSTGRESQL,
                lambda dt, days: dt + sa.cast(sa.cast(days, sa.TEXT) + " day", sa.Interval(second_precision=6)),
            ),
        ]
    ),
    # -
    base.BinaryMinusNumbers.for_dialect(D.POSTGRESQL),
    base.BinaryMinusDateInt(
        variants=[
            V(
                D.POSTGRESQL,
                lambda date, days: sa.cast(
                    sa.func.to_timestamp(sa.func.extract("epoch", date) - days * DAY_SEC), sa.Date
                ),
            ),
        ]
    ),
    base.BinaryMinusDateFloat(
        variants=[
            V(
                D.POSTGRESQL,
                lambda date, days: sa.cast(
                    sa.func.to_timestamp(sa.func.extract("epoch", date) - base.as_bigint(sa.func.CEIL(days)) * DAY_SEC),
                    sa.Date,
                ),
            ),
        ]
    ),
    base.BinaryMinusDatetimeNumber(
        variants=[
            V(D.POSTGRESQL, lambda dt, days: sa.func.to_timestamp(sa.func.extract("epoch", dt) - days * DAY_SEC)),
        ]
    ),
    base.BinaryMinusGenericDatetimeNumber(
        variants=[
            V(
                D.POSTGRESQL,
                lambda dt, days: dt - sa.cast(sa.cast(days, sa.TEXT) + " day", sa.Interval(second_precision=6)),
            ),
        ]
    ),
    base.BinaryMinusDates.for_dialect(D.POSTGRESQL),
    base.BinaryMinusDatetimes(
        variants=[
            V(
                D.POSTGRESQL,
                lambda left, right: ((sa.func.extract("epoch", left) - sa.func.extract("epoch", right)) / DAY_SEC),
            ),
        ]
    ),
    base.BinaryMinusGenericDatetimes(
        variants=[
            V(D.POSTGRESQL, lambda left, right: sa.func.extract("epoch", left - right) / DAY_SEC),
        ]
    ),
    # /
    base.BinaryDivInt(
        variants=[
            # Force safe division in COMPENG to make the behavior as permissive as possible
            V(D.COMPENG, lambda x, y: sa.cast(x, sa_postgresql.DOUBLE_PRECISION) / sa.func.nullif(y, 0)),
        ]
    ),
    base.BinaryDivFloat(
        variants=[
            # Force safe division in COMPENG to make the behavior as permissive as possible
            V(D.COMPENG, lambda x, y: x / sa.func.nullif(y, 0)),
        ]
    ),
    base.BinaryDivInt(
        variants=[
            V(D.NON_COMPENG_POSTGRESQL, lambda x, y: sa.cast(x, sa_postgresql.DOUBLE_PRECISION) / y),
        ]
    ),
    base.BinaryDivFloat.for_dialect(D.NON_COMPENG_POSTGRESQL),
    # <
    base.BinaryLessThan.for_dialect(D.POSTGRESQL),
    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.POSTGRESQL),
    # ==
    base.BinaryEqual.for_dialect(D.POSTGRESQL),
    # >
    base.BinaryGreaterThan.for_dialect(D.POSTGRESQL),
    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.POSTGRESQL),
    # ^
    base.BinaryPower.for_dialect(D.POSTGRESQL),
    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.POSTGRESQL),
    # _==
    base.BinaryEqualInternal.for_dialect(D.POSTGRESQL),
    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.POSTGRESQL),
    # and
    base.BinaryAnd.for_dialect(D.POSTGRESQL),
    # in
    base.BinaryIn.for_dialect(D.POSTGRESQL),
    # like
    base.BinaryLike.for_dialect(D.POSTGRESQL),
    # notin
    base.BinaryNotIn.for_dialect(D.POSTGRESQL),
    # notlike
    base.BinaryNotLike.for_dialect(D.POSTGRESQL),
    # or
    base.BinaryOr.for_dialect(D.POSTGRESQL),
]
