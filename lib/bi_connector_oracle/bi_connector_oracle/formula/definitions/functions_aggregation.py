import sqlalchemy as sa

from bi_formula.definitions.literals import un_literal
from bi_formula.definitions.common import quantile_value, within_group
import bi_formula.definitions.functions_aggregation as base
from bi_formula.definitions.base import (
    TranslationVariant,
)

from bi_connector_oracle.formula.constants import OracleDialect as D


V = TranslationVariant.make


DEFINITIONS_AGG = [
    # avg
    base.AggAvgFromNumber.for_dialect(D.ORACLE),
    base.AggAvgFromDate.for_dialect(D.ORACLE),
    base.AggAvgFromDatetime.for_dialect(D.ORACLE),
    base.AggAvgFromDatetimeTZ.for_dialect(D.ORACLE),

    # avg_if
    base.AggAvgIf.for_dialect(D.ORACLE),

    # count
    base.AggCount0.for_dialect(D.ORACLE),
    base.AggCount1.for_dialect(D.ORACLE),

    # count_if
    base.AggCountIf.for_dialect(D.ORACLE),

    # countd
    base.AggCountd.for_dialect(D.ORACLE),

    # countd_approx
    base.AggCountdApprox(variants=[
        V(D.ORACLE_12_0, sa.func.APPROX_COUNT_DISTINCT),
    ]),

    # countd_if
    base.AggCountdIf.for_dialect(D.ORACLE),

    # max
    base.AggMax.for_dialect(D.ORACLE),

    # median
    base.AggMedian(variants=[
        V(D.ORACLE, lambda expr: within_group(sa.func.PERCENTILE_DISC(0.5), expr)),
    ]),

    # min
    base.AggMin.for_dialect(D.ORACLE),

    # quantile
    base.AggQuantile(variants=[
        V(D.ORACLE, lambda expr, quant: within_group(
            sa.func.percentile_disc(quantile_value(un_literal(quant))), expr
        )),
    ]),

    # stdev
    base.AggStdev.for_dialect(D.ORACLE),

    # stdevp
    base.AggStdevp.for_dialect(D.ORACLE),

    # sum
    base.AggSum.for_dialect(D.ORACLE),

    # sum_if
    base.AggSumIf.for_dialect(D.ORACLE),

    # var
    base.AggVar.for_dialect(D.ORACLE),

    # varp
    base.AggVarp.for_dialect(D.ORACLE),
]
