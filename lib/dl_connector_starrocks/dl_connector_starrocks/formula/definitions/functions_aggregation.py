import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_aggregation as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


DEFINITIONS_AGG = [
    # arg_max
    base.AggArgMax(
        variants=[
            V(D.STARROCKS, sa.func.MAX_BY),
        ]
    ),
    # arg_min
    base.AggArgMin(
        variants=[
            V(D.STARROCKS, sa.func.MIN_BY),
        ]
    ),
    # any
    base.AggAny(
        variants=[
            V(D.STARROCKS, sa.func.ANY_VALUE),
        ]
    ),
    # avg
    base.AggAvgFromNumber.for_dialect(D.STARROCKS),
    base.AggAvgFromDate.for_dialect(D.STARROCKS),
    base.AggAvgFromDatetime(
        variants=[
            V(
                D.STARROCKS,
                lambda dt_val: sa.cast(
                    sa.func.FROM_UNIXTIME(sa.func.AVG(sa.func.UNIX_TIMESTAMP(dt_val))),
                    sa.DateTime(),
                ),
            ),
        ]
    ),
    base.AggAvgFromDatetimeTZ(
        variants=[
            V(
                D.STARROCKS,
                lambda dt: sa.cast(
                    sa.func.FROM_UNIXTIME(sa.func.AVG(sa.func.UNIX_TIMESTAMP(dt))),
                    sa.DateTime(),
                ),
            ),
        ]
    ),
    # avg_if
    base.AggAvgIf.for_dialect(D.STARROCKS),
    # count
    base.AggCount0.for_dialect(D.STARROCKS),
    base.AggCount1.for_dialect(D.STARROCKS),
    # count_if
    base.AggCountIf.for_dialect(D.STARROCKS),
    # countd
    base.AggCountd.for_dialect(D.STARROCKS),
    # countd_approx
    base.AggCountdApprox(
        variants=[
            V(D.STARROCKS, sa.func.APPROX_COUNT_DISTINCT),
        ]
    ),
    # countd_if
    base.AggCountdIf.for_dialect(D.STARROCKS),
    # max
    base.AggMax.for_dialect(D.STARROCKS),
    # min
    base.AggMin.for_dialect(D.STARROCKS),
    # stdev
    base.AggStdev.for_dialect(D.STARROCKS),
    # stdevp
    base.AggStdevp.for_dialect(D.STARROCKS),
    # sum
    base.AggSum.for_dialect(D.STARROCKS),
    # sum_if
    base.AggSumIf.for_dialect(D.STARROCKS),
    # var
    base.AggVar.for_dialect(D.STARROCKS),
    # varp
    base.AggVarp.for_dialect(D.STARROCKS),
]
