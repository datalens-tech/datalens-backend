from typing import (
    Any,
    Optional,
    cast,
)

import sqlalchemy as sa
from sqlalchemy.sql.elements import (
    ClauseElement,
    ClauseList,
)
from sqlalchemy.sql.functions import Function as SAFunction

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D
from dl_formula.definitions.base import (
    FuncTranslationImplementationBase,
    TranslateCallback,
    TranslationVariant,
)
import dl_formula.definitions.functions_window as base
from dl_formula.translation.context import TranslationCtx
from dl_formula.translation.env import TranslationEnvironment


V = TranslationVariant.make

SUPPORTED_DIALECTS = D.and_above(D.CLICKHOUSE_22_10)


class WinLagClickHouseBase(base.WinLagBase):
    variants = [
        V(
            SUPPORTED_DIALECTS,
            translation=lambda x, offset=sa.literal(1), default=sa.null(), *_: (
                base.lag_implementation(
                    x,
                    offset=offset,
                    default=default,
                    lag_name="lagInFrame",
                    lead_name="leadInFrame",
                )
            ),
            translation_rows=lambda x, offset=sa.literal(1), *_: (None, None),
            as_winfunc=True,
        ),
    ]


class WinLag1ClickHouse(WinLagClickHouseBase):
    arg_cnt = 1


class WinLag2ClickHouse(WinLagClickHouseBase):
    arg_cnt = 2


class WinLag3ClickHouse(WinLagClickHouseBase):
    arg_cnt = 3


class RankPercentileTranslationImplementation(FuncTranslationImplementationBase):
    def translate(
        self,
        *raw_args: TranslationCtx,
        translator_cb: TranslateCallback,
        partition_by: Optional[ClauseList] = None,
        default_order_by: Optional[ClauseList] = None,
        translation_ctx: Optional[TranslationCtx] = None,
        translation_env: Optional[TranslationEnvironment] = None,
    ) -> ClauseElement:
        def translation_rank(value: Any, *args: Any) -> SAFunction:
            return sa.func.RANK(value)

        args, kwargs = self._handle_args(
            raw_args,
            translation_func=translation_rank,
            translation_ctx=translation_ctx,
            translation_env=translation_env,
        )
        order_by_part = base._order_by_from_args(*args)  # type: ignore

        wf_rank = translation_rank(*args).over(partition_by=partition_by, order_by=order_by_part)
        wf_total_partition_rows = sa.func.COUNT(1).over(partition_by=partition_by)  # ORDER BY is unnecessary

        result = (wf_rank - 1) / (wf_total_partition_rows - 1)
        return cast(ClauseElement, result)


class WinRankPercentileClickHouseBase(base.WinRankPercentileBase):
    variants = [
        TranslationVariant(
            dialects=SUPPORTED_DIALECTS,
            value=RankPercentileTranslationImplementation(),
        ),
    ]


class WinRankPercentile1ClickHouse(WinRankPercentileClickHouseBase):
    arg_cnt = 1


class WinRankPercentile2ClickHouse(WinRankPercentileClickHouseBase):
    arg_cnt = 2


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
    WinLag1ClickHouse(),
    WinLag2ClickHouse(),
    WinLag3ClickHouse(),
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
    WinRankPercentile1ClickHouse(),
    WinRankPercentile2ClickHouse(),
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
