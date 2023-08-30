import sqlalchemy as sa
from bi_formula.definitions.literals import un_literal

from bi_formula.definitions.common import quantile_value

from bi_formula.core.datatype import DataType

from bi_formula.definitions.args import ArgTypeSequence
from sqlalchemy import within_group

import bi_formula.definitions.functions_aggregation as base
from bi_formula.definitions.base import (
    TranslationVariant,
)

from bi_connector_snowflake.formula.constants import SnowFlakeDialect as D


V = TranslationVariant.make


class AggMedianIntSF(base.AggMedian):
    name = "median"
    argument_types = [
        ArgTypeSequence([{DataType.INTEGER}]),
    ]
    variants = [
        V(
            D.SNOWFLAKE,
            lambda expr: sa.func.TO_NUMBER(
                within_group(sa.func.PERCENTILE_DISC(0.5), expr),
            ),
        ),
    ]


class AggMedianFloatSF(base.AggMedian):
    name = "median"
    argument_types = [
        ArgTypeSequence([{DataType.FLOAT}]),
    ]
    variants = [
        V(
            D.SNOWFLAKE,
            lambda expr: within_group(sa.func.PERCENTILE_DISC(0.5), expr),
        ),
    ]


class AggMedianDateSF(base.AggMedian):
    name = "median"
    argument_types = [ArgTypeSequence([{DataType.DATE}])]
    variants = [
        V(
            D.SNOWFLAKE,
            lambda expr: sa.func.TO_DATE(
                sa.func.TO_TIMESTAMP(
                    within_group(
                        sa.func.PERCENTILE_DISC(0.5),
                        sa.func.DATE_PART("EPOCH_SECOND", expr),
                    )
                )
            ),
        ),
    ]


class AggMedianDateTimeSF(base.AggMedian):
    name = "median"
    argument_types = [ArgTypeSequence([{DataType.DATETIME, DataType.GENERICDATETIME}])]
    variants = [
        V(
            D.SNOWFLAKE,
            lambda expr: sa.func.TO_TIMESTAMP(
                within_group(
                    sa.func.PERCENTILE_DISC(0.5),
                    sa.func.DATE_PART("EPOCH_SECOND", expr),
                )
            ),
        ),
    ]


DEFINITIONS_AGG = [
    # any
    base.AggAny(
        variants=[
            V(D.SNOWFLAKE, sa.func.ANY_VALUE),
        ]
    ),

    # avg
    base.AggAvgFromNumber.for_dialect(D.SNOWFLAKE),
    # base.AggAvgFromDate.for_dialect(D.SNOWFLAKE),
    base.AggAvgFromDate(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda date_val: sa.func.TO_DATE(
                    sa.func.TO_TIMESTAMP(
                        sa.func.AVG(sa.func.DATE_PART("EPOCH_SECOND", date_val)),
                    )
                ),
            )
        ]
    ),
    base.AggAvgFromDatetime.for_dialect(D.SNOWFLAKE),
    base.AggAvgFromDatetimeTZ.for_dialect(D.SNOWFLAKE),

    # avg_if
    base.AggAvgIf.for_dialect(D.SNOWFLAKE),

    # count
    base.AggCount0.for_dialect(D.SNOWFLAKE),
    base.AggCount1.for_dialect(D.SNOWFLAKE),

    # count_if
    base.AggCountIf(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda expr: sa.func.IFNULL(sa.func.COUNT_IF(expr), 0),
            )
        ]
    ),

    # countd
    base.AggCountd.for_dialect(D.SNOWFLAKE),

    # countd_approx
    base.AggCountdApprox(
        variants=[
            V(D.SNOWFLAKE, sa.func.APPROX_COUNT_DISTINCT),
        ]
    ),

    # countd_if
    base.AggCountdIf.for_dialect(D.SNOWFLAKE),

    # max
    base.AggMax.for_dialect(D.SNOWFLAKE),

    # median
    AggMedianIntSF.for_dialect(D.SNOWFLAKE),
    AggMedianFloatSF.for_dialect(D.SNOWFLAKE),
    AggMedianDateSF.for_dialect(D.SNOWFLAKE),
    AggMedianDateTimeSF.for_dialect(D.SNOWFLAKE),

    # quantile
    base.AggQuantile(
        variants=[
            V(D.SNOWFLAKE, lambda expr, quant: within_group(sa.func.percentile_disc(quantile_value(un_literal(quant))), expr)),
        ]
    ),

    # min
    base.AggMin.for_dialect(D.SNOWFLAKE),

    # stdev
    base.AggStdev.for_dialect(D.SNOWFLAKE),

    # stdevp
    base.AggStdevp.for_dialect(D.SNOWFLAKE),

    # sum
    base.AggSum.for_dialect(D.SNOWFLAKE),

    # sum_if
    base.AggSumIf.for_dialect(D.SNOWFLAKE),

    # var
    base.AggVar.for_dialect(D.SNOWFLAKE),

    # varp
    base.AggVarp.for_dialect(D.SNOWFLAKE),
]
