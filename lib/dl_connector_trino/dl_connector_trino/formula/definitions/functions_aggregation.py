
from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_aggregation as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


DEFINITIONS_AGG = [
    # all_concat
    # base.AggAllConcat1.for_dialect(D.TRINO),
    # base.AggAllConcat2.for_dialect(D.TRINO),
    # any
    # base.AggAny.for_dialect(D.TRINO),
    # arg_max
    # base.AggArgMax.for_dialect(D.TRINO),
    # arg_min
    # base.AggArgMin.for_dialect(D.TRINO),
    # avg
    base.AggAvgFromNumber.for_dialect(D.TRINO),
    # base.AggAvgFromDate.for_dialect(D.TRINO),
    # base.AggAvgFromDatetime.for_dialect(D.TRINO),
    # base.AggAvgFromDatetimeTZ.for_dialect(D.TRINO),
    # avg_if
    # base.AggAvgIf.for_dialect(D.TRINO),
    # count
    base.AggCount0.for_dialect(D.TRINO),
    base.AggCount1.for_dialect(D.TRINO),
    # count_if
    # base.AggCountIf.for_dialect(D.TRINO),
    # countd
    base.AggCountd.for_dialect(D.TRINO),
    # countd_approx
    # base.AggCountdApprox.for_dialect(D.TRINO),
    # countd_if
    # base.AggCountdIf.for_dialect(D.TRINO),
    # max
    base.AggMax.for_dialect(D.TRINO),
    # median
    # base.AggMedian.for_dialect(D.TRINO),
    # min
    base.AggMin.for_dialect(D.TRINO),
    # quantile
    # base.AggQuantile.for_dialect(D.TRINO),
    # quantile_approx
    # base.AggQuantileApproximate.for_dialect(D.TRINO),
    # stdev
    # base.AggStdev.for_dialect(D.TRINO),
    # stdevp
    # base.AggStdevp.for_dialect(D.TRINO),
    # sum
    base.AggSum.for_dialect(D.TRINO),
    # sum_if
    # base.AggSumIf.for_dialect(D.TRINO),
    # top_concat
    # base.AggTopConcat1.for_dialect(D.TRINO),
    # base.AggTopConcat2.for_dialect(D.TRINO),
    # var
    # base.AggVar.for_dialect(D.TRINO),
    # varp
    # base.AggVarp.for_dialect(D.TRINO),
]
