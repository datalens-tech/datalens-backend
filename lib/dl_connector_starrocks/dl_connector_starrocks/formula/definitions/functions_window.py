import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_window as base
from dl_formula.definitions.functions_window import lag_implementation

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


DEFINITIONS_WINDOW = [
    # avg
    # StarRocks requires ORDER BY with ROWS frame, so omit translation_rows for TOTAL window
    base.WinAvg(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x: sa.func.AVG(x),
                as_winfunc=True,
            ),
        ]
    ),
    # avg_if
    base.WinAvgIf(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x, condition: sa.func.AVG(sa.func.IF(condition, x, None)),
                as_winfunc=True,
            ),
        ]
    ),
    # count
    base.WinCount0(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda: sa.func.COUNT(sa.text("*")),
                as_winfunc=True,
            ),
        ]
    ),
    base.WinCount1(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x: sa.func.COUNT(x),
                as_winfunc=True,
            ),
        ]
    ),
    # count_if
    base.WinCountIf(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x, condition: sa.func.COUNT(sa.func.IF(condition, x, None)),
                as_winfunc=True,
            ),
        ]
    ),
    # first
    base.WinFirst.for_dialect(D.STARROCKS),
    # lag
    # StarRocks does not allow ROWS frame with LAG, so omit translation_rows
    base.WinLag1(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x, offset=sa.literal(1), default=sa.null(), *_: (  # noqa: B008
                    lag_implementation(x, offset=offset, default=default)
                ),
                as_winfunc=True,
            ),
        ]
    ),
    base.WinLag2(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x, offset=sa.literal(1), default=sa.null(), *_: (  # noqa: B008
                    lag_implementation(x, offset=offset, default=default)
                ),
                as_winfunc=True,
            ),
        ]
    ),
    base.WinLag3(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x, offset=sa.literal(1), default=sa.null(), *_: (  # noqa: B008
                    lag_implementation(x, offset=offset, default=default)
                ),
                as_winfunc=True,
            ),
        ]
    ),
    # last
    base.WinLast.for_dialect(D.STARROCKS),
    # mavg
    base.WinMAvg2.for_dialect(D.STARROCKS),
    base.WinMAvg3.for_dialect(D.STARROCKS),
    # max
    base.WinMax(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x: sa.func.MAX(x),
                as_winfunc=True,
            ),
        ]
    ),
    # mcount
    base.WinMCount2.for_dialect(D.STARROCKS),
    base.WinMCount3.for_dialect(D.STARROCKS),
    # min
    base.WinMin(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x: sa.func.MIN(x),
                as_winfunc=True,
            ),
        ]
    ),
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
    base.WinSum(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x: sa.func.SUM(x),
                as_winfunc=True,
            ),
        ]
    ),
    # sum_if
    base.WinSumIf(
        variants=[
            V(
                D.STARROCKS,
                translation=lambda x, condition: sa.func.SUM(sa.func.IF(condition, x, None)),
                as_winfunc=True,
            ),
        ]
    ),
]
