import dl_formula.definitions.functions_window as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


DEFINITIONS_WINDOW = [
    # TODO: BI-7171 WinAvg, WinAvgIf (StarRocks requires ORDER BY for ROWS frame)
    # TODO: BI-7171 WinCount0, WinCount1, WinCountIf (StarRocks requires ORDER BY for ROWS frame)
    # TODO: BI-7171 WinMax, WinMin, WinSum, WinSumIf (StarRocks requires ORDER BY for ROWS frame)
    # TODO: BI-7171 WinLag1, WinLag2, WinLag3 (StarRocks does not allow ROWS frame with LAG)
    # first
    base.WinFirst.for_dialect(D.STARROCKS),
    # last
    base.WinLast.for_dialect(D.STARROCKS),
    # mavg
    base.WinMAvg2.for_dialect(D.STARROCKS),
    base.WinMAvg3.for_dialect(D.STARROCKS),
    # mcount
    base.WinMCount2.for_dialect(D.STARROCKS),
    base.WinMCount3.for_dialect(D.STARROCKS),
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
]
