import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import raw_sql
from dl_formula.definitions.common_datetime import DAY_SEC
import dl_formula.definitions.operators_binary as base

from dl_connector_bigquery.formula.constants import BigQueryDialect as D


V = TranslationVariant.make


def _bq_sec_interval(sec: ClauseElement) -> ClauseElement:
    return sa.func.MAKE_INTERVAL(0, 0, 0, 0, 0, sec)


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.BIGQUERY),
    # %
    base.BinaryModInteger(
        variants=[
            V(D.BIGQUERY, sa.func.MOD),
        ]
    ),
    base.BinaryModFloat(
        variants=[
            V(D.BIGQUERY, lambda left, right: left - sa.func.FLOOR(left / right) * right),
        ]
    ),
    # *
    base.BinaryMultNumbers.for_dialect(D.BIGQUERY),
    base.BinaryMultStringConst.for_dialect(D.BIGQUERY),
    base.BinaryMultStringNonConst(
        variants=[
            V(D.BIGQUERY, sa.func.REPEAT),
        ]
    ),
    # +
    base.BinaryPlusNumbers.for_dialect(D.BIGQUERY),
    base.BinaryPlusStrings.for_dialect(D.BIGQUERY),
    base.BinaryPlusDateInt(
        variants=[
            V(D.BIGQUERY, lambda date, days: date + days),
        ]
    ),
    base.BinaryPlusDateFloat(
        variants=[
            V(D.BIGQUERY, lambda date, days: date + sa.cast(days, sa.INTEGER)),
        ]
    ),
    base.BinaryPlusDatetimeNumber(
        variants=[
            V(D.BIGQUERY, lambda date, days: date + _bq_sec_interval(sa.cast(days * DAY_SEC, sa.INTEGER))),
        ]
    ),
    base.BinaryPlusGenericDatetimeNumber(
        variants=[
            V(D.BIGQUERY, lambda date, days: date + _bq_sec_interval(sa.cast(days * DAY_SEC, sa.INTEGER))),
        ]
    ),
    # -
    base.BinaryMinusNumbers.for_dialect(D.BIGQUERY),
    base.BinaryMinusDateInt(
        variants=[
            V(D.BIGQUERY, lambda date, days: date - days),
        ]
    ),
    base.BinaryMinusDateFloat(
        variants=[
            V(
                D.BIGQUERY,
                lambda date, days: sa.cast(
                    sa.cast(date, sa.DATETIME) - _bq_sec_interval(sa.cast(days * DAY_SEC, sa.INTEGER)), sa.DATE
                ),
            ),
        ]
    ),
    base.BinaryMinusDatetimeNumber(
        variants=[
            V(
                D.BIGQUERY,
                lambda date, days: sa.cast(date, sa.DATETIME) - _bq_sec_interval(sa.cast(days * DAY_SEC, sa.INTEGER)),
            ),
        ]
    ),
    base.BinaryMinusGenericDatetimeNumber(
        variants=[
            V(D.BIGQUERY, lambda date, days: date - _bq_sec_interval(sa.cast(days * DAY_SEC, sa.INTEGER))),
        ]
    ),
    base.BinaryMinusDates(
        variants=[
            V(D.BIGQUERY, lambda left, right: sa.func.EXTRACT(raw_sql("DAY"), left - right)),
        ]
    ),
    base.BinaryMinusDatetimes(
        variants=[
            V(
                D.BIGQUERY,
                lambda left, right: (
                    sa.func.UNIX_SECONDS(sa.cast(left, sa.TIMESTAMP))
                    - sa.func.UNIX_SECONDS(sa.cast(right, sa.TIMESTAMP))
                )
                / DAY_SEC,
            ),
        ]
    ),
    base.BinaryMinusGenericDatetimes(
        variants=[
            V(
                D.BIGQUERY,
                lambda left, right: (
                    sa.func.UNIX_SECONDS(sa.cast(left, sa.TIMESTAMP))
                    - sa.func.UNIX_SECONDS(sa.cast(right, sa.TIMESTAMP))
                )
                / DAY_SEC,
            ),
        ]
    ),
    # /
    base.BinaryDivInt.for_dialect(D.BIGQUERY),
    base.BinaryDivFloat.for_dialect(D.BIGQUERY),
    # <
    base.BinaryLessThan.for_dialect(D.BIGQUERY),
    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.BIGQUERY),
    # ==
    base.BinaryEqual.for_dialect(D.BIGQUERY),
    # >
    base.BinaryGreaterThan.for_dialect(D.BIGQUERY),
    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.BIGQUERY),
    # ^
    base.BinaryPower.for_dialect(D.BIGQUERY),
    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.BIGQUERY),
    # _==
    base.BinaryEqualInternal.for_dialect(D.BIGQUERY),
    # _dneq
    base.BinaryEqualDenullified.for_dialect(D.BIGQUERY),
    # and
    base.BinaryAnd.for_dialect(D.BIGQUERY),
    # in
    base.BinaryIn.for_dialect(D.BIGQUERY),
    # like
    base.BinaryLike.for_dialect(D.BIGQUERY),
    # notin
    base.BinaryNotIn.for_dialect(D.BIGQUERY),
    # notlike
    base.BinaryNotLike.for_dialect(D.BIGQUERY),
    # or
    base.BinaryOr.for_dialect(D.BIGQUERY),
]
