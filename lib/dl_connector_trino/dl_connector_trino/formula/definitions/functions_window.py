import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_window as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


class WinLagTrinoBase(base.WinLagBase):
    variants = [
        V(
            D.TRINO,
            translation=lambda x, offset=None, default=None, *_: base.lag_implementation(
                x,
                offset=offset if offset is not None else sa.literal(1),
                default=default if default is not None else sa.null(),
            ),
            as_winfunc=True,
        ),
    ]


class WinLag1Trino(WinLagTrinoBase):
    arg_cnt = 1


class WinLag2Trino(WinLagTrinoBase):
    arg_cnt = 2


class WinLag3Trino(WinLagTrinoBase):
    arg_cnt = 3


DEFINITIONS_WINDOW = [
    # avg
    base.WinAvg.for_dialect(D.TRINO),
    # avg_if
    # base.WinAvgIf.for_dialect(D.TRINO),
    # count
    base.WinCount0.for_dialect(D.TRINO),
    base.WinCount1.for_dialect(D.TRINO),
    # count_if
    # base.WinCountIf.for_dialect(D.TRINO),
    # first
    base.WinFirst.for_dialect(D.TRINO),
    # lag
    WinLag1Trino(),
    WinLag2Trino(),
    WinLag3Trino(),
    # last
    base.WinLast.for_dialect(D.TRINO),
    # mavg
    base.WinMAvg2.for_dialect(D.TRINO),
    base.WinMAvg3.for_dialect(D.TRINO),
    # max
    base.WinMax.for_dialect(D.TRINO),
    # mcount
    base.WinMCount2.for_dialect(D.TRINO),
    base.WinMCount3.for_dialect(D.TRINO),
    # min
    base.WinMin.for_dialect(D.TRINO),
    # mmax
    base.WinMMax2.for_dialect(D.TRINO),
    base.WinMMax3.for_dialect(D.TRINO),
    # mmin
    base.WinMMin2.for_dialect(D.TRINO),
    base.WinMMin3.for_dialect(D.TRINO),
    # msum
    base.WinMSum2.for_dialect(D.TRINO),
    base.WinMSum3.for_dialect(D.TRINO),
    # rank
    base.WinRank1.for_dialect(D.TRINO),
    base.WinRank2.for_dialect(D.TRINO),
    # rank_dense
    base.WinRankDense1.for_dialect(D.TRINO),
    base.WinRankDense2.for_dialect(D.TRINO),
    # rank_percentile
    base.WinRankPercentile1.for_dialect(D.TRINO),
    base.WinRankPercentile2.for_dialect(D.TRINO),
    # rank_unique
    base.WinRankUnique1.for_dialect(D.TRINO),
    base.WinRankUnique2.for_dialect(D.TRINO),
    # ravg
    base.WinRAvg1.for_dialect(D.TRINO),
    base.WinRAvg2.for_dialect(D.TRINO),
    # rcount
    base.WinRCount1.for_dialect(D.TRINO),
    base.WinRCount2.for_dialect(D.TRINO),
    # rmax
    base.WinRMax1.for_dialect(D.TRINO),
    base.WinRMax2.for_dialect(D.TRINO),
    # rmin
    base.WinRMin1.for_dialect(D.TRINO),
    base.WinRMin2.for_dialect(D.TRINO),
    # rsum
    base.WinRSum1.for_dialect(D.TRINO),
    base.WinRSum2.for_dialect(D.TRINO),
    # sum
    base.WinSum.for_dialect(D.TRINO),
    # sum_if
    # base.WinSumIf.for_dialect(D.TRINO),
]
