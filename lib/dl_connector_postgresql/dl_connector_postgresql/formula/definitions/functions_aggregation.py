import sqlalchemy as sa
from sqlalchemy.sql import ClauseElement

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import (
    quantile_value,
    within_group,
)
import dl_formula.definitions.functions_aggregation as base
from dl_formula.definitions.literals import un_literal

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D


V = TranslationVariant.make


def _all_concat_pg(expr: ClauseElement, sep: str = ", ") -> ClauseElement:
    res = expr
    res = res.distinct()  # type: ignore  # 2024-01-24 # TODO: "ClauseElement" has no attribute "distinct"  [attr-defined]
    # This could be
    # `res = sa.func.string_agg(res, sep)`
    # but older postgres versions refuse to auto-cast into strings that way.
    # Also notable: `json_agg`, for unambiguous representation.
    res = sa.func.array_agg(res)
    res = sa.func.array_to_string(res, sep)
    return res


DEFINITIONS_AGG = [
    # all_concat
    base.AggAllConcat1(
        variants=[
            V(D.POSTGRESQL, _all_concat_pg),
        ]
    ),
    base.AggAllConcat2(
        variants=[
            V(D.POSTGRESQL, _all_concat_pg),
        ]
    ),
    # any
    base.AggAny(
        variants=[
            # Only in COMPENG.
            # For compatibility with CH and other dialects that support the ANY aggregation
            V(D.COMPENG, lambda value: sa.func.max(value)),
        ]
    ),
    # avg
    base.AggAvgFromNumber.for_dialect(D.POSTGRESQL),
    base.AggAvgFromDate.for_dialect(D.POSTGRESQL),
    base.AggAvgFromDatetime.for_dialect(D.POSTGRESQL),
    base.AggAvgFromDatetimeTZ.for_dialect(D.POSTGRESQL),
    # avg_if
    base.AggAvgIf.for_dialect(D.POSTGRESQL),
    # count
    base.AggCount0.for_dialect(D.POSTGRESQL),
    base.AggCount1.for_dialect(D.POSTGRESQL),
    # count_if
    base.AggCountIf.for_dialect(D.POSTGRESQL),
    # countd
    base.AggCountd.for_dialect(D.POSTGRESQL),
    # countd_if
    base.AggCountdIf.for_dialect(D.POSTGRESQL),
    # max
    base.AggMax.for_dialect(D.POSTGRESQL),
    # median
    base.AggMedian(
        variants=[
            V(
                D.and_above(D.POSTGRESQL_9_4) | D.COMPENG,
                lambda expr: (within_group(sa.func.PERCENTILE_DISC(0.5), expr)),
            ),
        ]
    ),
    # min
    base.AggMin.for_dialect(D.POSTGRESQL),
    # quantile
    base.AggQuantile(
        variants=[
            V(
                D.and_above(D.POSTGRESQL_9_4) | D.COMPENG,
                lambda expr, quant: (
                    # In MSSQLSRV window functions cannot be used as aggregations
                    within_group(
                        sa.func.percentile_disc(quantile_value(un_literal(quant))),
                        expr,
                    )
                ),
            ),
        ]
    ),
    # stdev
    base.AggStdev.for_dialect(D.POSTGRESQL),
    # stdevp
    base.AggStdevp.for_dialect(D.POSTGRESQL),
    # sum
    base.AggSum.for_dialect(D.POSTGRESQL),
    # sum_if
    base.AggSumIf.for_dialect(D.POSTGRESQL),
    # var
    base.AggVar.for_dialect(D.POSTGRESQL),
    # varp
    base.AggVarp.for_dialect(D.POSTGRESQL),
]
