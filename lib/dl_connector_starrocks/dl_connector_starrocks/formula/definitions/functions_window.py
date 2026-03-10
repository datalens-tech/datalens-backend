import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_window as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


DEFINITIONS_WINDOW = [
    # avg
    base.WinAvg.for_dialect(D.STARROCKS),
    # avg_if
    base.WinAvgIf(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x, condition: sa.func.AVG(sa.func.IF(condition, x, None)),
                translation_rows=base._rows_full_window,  # noqa
                as_winfunc=True,
            ),
        ]
    ),
    # count
    base.WinCount0.for_dialect(D.STARROCKS),
    base.WinCount1.for_dialect(D.STARROCKS),
    # count_if
    base.WinCountIf(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x, condition: sa.func.COUNT(sa.func.IF(condition, x, None)),
                translation_rows=base._rows_full_window,  # noqa
                as_winfunc=True,
            ),
        ]
    ),
    # first
    base.WinFirst.for_dialect(D.STARROCKS),
    # lag
    base.WinLag1.for_dialect(D.STARROCKS),
    base.WinLag2.for_dialect(D.STARROCKS),
    base.WinLag3.for_dialect(D.STARROCKS),
    # last
    base.WinLast.for_dialect(D.STARROCKS),
    # mavg
    base.WinMAvg2.for_dialect(D.STARROCKS),
    base.WinMAvg3.for_dialect(D.STARROCKS),
    # max
    base.WinMax.for_dialect(D.STARROCKS),
    # mcount
    base.WinMCount2.for_dialect(D.STARROCKS),
    base.WinMCount3.for_dialect(D.STARROCKS),
    # min
    base.WinMin.for_dialect(D.STARROCKS),
    # mmax
    base.WinMMax2.for_dialect(D.STARROCKS),
    base.WinMMax3.for_dialect(D.STARROCKS),
    # mmin
    base.WinMMin2.for_dialect(D.STARROCKS),
    base.WinMMin3.for_dialect(D.STARROCKS),
    # msum
    base.WinMSum2.for_dialect(D.STARROCKS),
    base.WinMSum3.for_dialect(D.STARROCKS),
    # rank
    base.WinRank1.for_dialect(D.STARROCKS),
    base.WinRank2.for_dialect(D.STARROCKS),
    # rank_dense
    base.WinRankDense1.for_dialect(D.STARROCKS),
    base.WinRankDense2.for_dialect(D.STARROCKS),
    # rank_percentile
    base.WinRankPercentile1.for_dialect(D.STARROCKS),
    base.WinRankPercentile2.for_dialect(D.STARROCKS),
    # rank_unique
    base.WinRankUnique1.for_dialect(D.STARROCKS),
    base.WinRankUnique2.for_dialect(D.STARROCKS),
    # ravg
    base.WinRAvg1.for_dialect(D.STARROCKS),
    base.WinRAvg2.for_dialect(D.STARROCKS),
    # rcount
    base.WinRCount1.for_dialect(D.STARROCKS),
    base.WinRCount2.for_dialect(D.STARROCKS),
    # rmax
    base.WinRMax1.for_dialect(D.STARROCKS),
    base.WinRMax2.for_dialect(D.STARROCKS),
    # rmin
    base.WinRMin1.for_dialect(D.STARROCKS),
    base.WinRMin2.for_dialect(D.STARROCKS),
    # rsum
    base.WinRSum1.for_dialect(D.STARROCKS),
    base.WinRSum2.for_dialect(D.STARROCKS),
    # sum
    base.WinSum.for_dialect(D.STARROCKS),
    # sum_if
    base.WinSumIf(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x, condition: sa.func.SUM(sa.func.IF(condition, x, None)),
                translation_rows=base._rows_full_window,  # noqa
                as_winfunc=True,
            ),
        ]
    ),
]
