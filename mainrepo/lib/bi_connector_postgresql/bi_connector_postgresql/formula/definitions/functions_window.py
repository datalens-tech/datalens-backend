import bi_formula.definitions.functions_window as base

from bi_connector_postgresql.formula.constants import PostgreSQLDialect as D

SUPPORTED_DIALECTS = D.COMPENG | D.and_above(D.POSTGRESQL_9_4)


DEFINITIONS_WINDOW = [
    # avg
    base.WinAvg.for_dialect(SUPPORTED_DIALECTS),
    # avg_if
    base.WinAvgIf.for_dialect(SUPPORTED_DIALECTS),
    # count
    base.WinCount0.for_dialect(SUPPORTED_DIALECTS),
    base.WinCount1.for_dialect(SUPPORTED_DIALECTS),
    # count_if
    base.WinCountIf.for_dialect(SUPPORTED_DIALECTS),
    # first
    base.WinFirst.for_dialect(SUPPORTED_DIALECTS),
    # lag
    base.WinLag1.for_dialect(SUPPORTED_DIALECTS),
    base.WinLag2.for_dialect(SUPPORTED_DIALECTS),
    base.WinLag3.for_dialect(SUPPORTED_DIALECTS),
    # last
    base.WinLast.for_dialect(SUPPORTED_DIALECTS),
    # mavg
    base.WinMAvg2.for_dialect(SUPPORTED_DIALECTS),
    base.WinMAvg3.for_dialect(SUPPORTED_DIALECTS),
    # max
    base.WinMax.for_dialect(SUPPORTED_DIALECTS),
    # mcount
    base.WinMCount2.for_dialect(SUPPORTED_DIALECTS),
    base.WinMCount3.for_dialect(SUPPORTED_DIALECTS),
    # min
    base.WinMin.for_dialect(SUPPORTED_DIALECTS),
    # mmax
    base.WinMMax2.for_dialect(SUPPORTED_DIALECTS),
    base.WinMMax3.for_dialect(SUPPORTED_DIALECTS),
    # mmin
    base.WinMMin2.for_dialect(SUPPORTED_DIALECTS),
    base.WinMMin3.for_dialect(SUPPORTED_DIALECTS),
    # msum
    base.WinMSum2.for_dialect(SUPPORTED_DIALECTS),
    base.WinMSum3.for_dialect(SUPPORTED_DIALECTS),
    # rank
    base.WinRank1.for_dialect(SUPPORTED_DIALECTS),
    base.WinRank2.for_dialect(SUPPORTED_DIALECTS),
    # rank_dense
    base.WinRankDense1.for_dialect(SUPPORTED_DIALECTS),
    base.WinRankDense2.for_dialect(SUPPORTED_DIALECTS),
    # rank_percentile
    base.WinRankPercentile1.for_dialect(SUPPORTED_DIALECTS),
    base.WinRankPercentile2.for_dialect(SUPPORTED_DIALECTS),
    # rank_unique
    base.WinRankUnique1.for_dialect(SUPPORTED_DIALECTS),
    base.WinRankUnique2.for_dialect(SUPPORTED_DIALECTS),
    # ravg
    base.WinRAvg1.for_dialect(SUPPORTED_DIALECTS),
    base.WinRAvg2.for_dialect(SUPPORTED_DIALECTS),
    # rcount
    base.WinRCount1.for_dialect(SUPPORTED_DIALECTS),
    base.WinRCount2.for_dialect(SUPPORTED_DIALECTS),
    # rmax
    base.WinRMax1.for_dialect(SUPPORTED_DIALECTS),
    base.WinRMax2.for_dialect(SUPPORTED_DIALECTS),
    # rmin
    base.WinRMin1.for_dialect(SUPPORTED_DIALECTS),
    base.WinRMin2.for_dialect(SUPPORTED_DIALECTS),
    # rsum
    base.WinRSum1.for_dialect(SUPPORTED_DIALECTS),
    base.WinRSum2.for_dialect(SUPPORTED_DIALECTS),
    # sum
    base.WinSum.for_dialect(SUPPORTED_DIALECTS),
    # sum_if
    base.WinSumIf.for_dialect(SUPPORTED_DIALECTS),
]
