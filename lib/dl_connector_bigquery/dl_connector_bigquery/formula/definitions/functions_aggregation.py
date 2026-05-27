import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_aggregation as base

from dl_connector_bigquery.formula.constants import BigQueryDialect as D

V = TranslationVariant.make


DEFINITIONS_AGG = [
    # any
    base.AggAny(
        variants=[
            V(D.BIGQUERY, sa.func.ANY_VALUE),
        ]
    ),
    # avg
    base.AggAvgFromNumber.for_dialect(D.BIGQUERY),
    base.AggAvgFromDate.for_dialect(D.BIGQUERY),
    base.AggAvgFromDatetime.for_dialect(D.BIGQUERY),
    base.AggAvgFromDatetimeTZ.for_dialect(D.BIGQUERY),
    # avg_if
    base.AggAvgIf.for_dialect(D.BIGQUERY),
    # count
    base.AggCount0.for_dialect(D.BIGQUERY),
    base.AggCount1.for_dialect(D.BIGQUERY),
    # count_if
    base.AggCountIf(variants=[V(D.BIGQUERY, sa.func.COUNTIF)]),
    # countd
    base.AggCountd.for_dialect(D.BIGQUERY),
    # countd_approx
    base.AggCountdApprox(
        variants=[
            V(D.BIGQUERY, sa.func.APPROX_COUNT_DISTINCT),
        ]
    ),
    # countd_if
    base.AggCountdIf.for_dialect(D.BIGQUERY),
    # max
    base.AggMax.for_dialect(D.BIGQUERY),
    # min
    base.AggMin.for_dialect(D.BIGQUERY),
    # stdev
    base.AggStdev.for_dialect(D.BIGQUERY),
    # stdevp
    base.AggStdevp.for_dialect(D.BIGQUERY),
    # sum
    base.AggSum.for_dialect(D.BIGQUERY),
    # sum_if
    base.AggSumIf.for_dialect(D.BIGQUERY),
    # var
    base.AggVar.for_dialect(D.BIGQUERY),
    # varp
    base.AggVarp.for_dialect(D.BIGQUERY),
]
