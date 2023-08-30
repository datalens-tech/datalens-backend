import sqlalchemy as sa
from sqlalchemy.sql import ClauseElement

import clickhouse_sqlalchemy.ext.clauses as chsq_clauses

import bi_formula.definitions.functions_aggregation as base
from bi_formula.definitions.base import (
    TranslationVariant,
)
from bi_formula.definitions.common import quantile_value
from bi_formula.definitions.literals import literal, un_literal
from bi_formula.shortcuts import n

from bi_connector_clickhouse.formula.constants import ClickHouseDialect as D


V = TranslationVariant.make


def _explicit_ch_tostring(value: ClauseElement) -> ClauseElement:
    """
    A hax-fix function that
    1. has explicit arguments, for CHSA Lambda
    2. is pickleable (not a lambda)
    """
    return sa.func.toString(value)


def _all_concat_ch(expr: ClauseElement, sep: str = ', ') -> ClauseElement:
    res = expr
    # Any --agg-> Array(Any)
    res = sa.func.groupUniqArray(res)
    # Reduce the stochasticity.
    res = sa.func.arraySort(res)
    # Array(Any) -> Array(String)
    res = sa.func.arrayMap(
        chsq_clauses.Lambda(_explicit_ch_tostring),
        res)
    # Array(String) -> String
    res = sa.func.arrayStringConcat(res, sep)
    return res


def _top_concat_ch(expr: ClauseElement, amount: ClauseElement, sep: str = ', ') -> ClauseElement:
    # TODO?: validate `amount > 0`?
    res = expr
    # Any --agg-> Array(Any)
    res = chsq_clauses.ParametrizedFunction(
        'topK',
        [amount],
        expr)
    # Result is frequency-ordered, so not sorting it, even though it can be
    # stochastic for equal frequencies.
    # Array(Any) -> Array(String)
    res = sa.func.arrayMap(
        chsq_clauses.Lambda(lambda x: sa.func.toString(x)),
        res)
    # Array(String) -> String
    res = sa.func.arrayStringConcat(res, sep)
    return res


DEFINITIONS_AGG = [
    # all_concat
    base.AggAllConcat1(variants=[
        V(D.CLICKHOUSE, _all_concat_ch),
    ]),
    base.AggAllConcat2(variants=[
        V(D.CLICKHOUSE, _all_concat_ch),
    ]),

    # any
    base.AggAny(variants=[
        V(D.CLICKHOUSE, sa.func.any),
    ]),

    # arg_max
    base.AggArgMax(variants=[
        V(D.CLICKHOUSE, sa.func.argMax),
    ]),

    # arg_min
    base.AggArgMin(variants=[
        V(D.CLICKHOUSE, sa.func.argMin),
    ]),

    # avg
    base.AggAvgFromNumber.for_dialect(D.CLICKHOUSE),
    base.AggAvgFromDate.for_dialect(D.CLICKHOUSE),
    base.AggAvgFromDatetime.for_dialect(D.CLICKHOUSE),
    base.AggAvgFromDatetimeTZ.for_dialect(D.CLICKHOUSE),

    # avg_if
    base.AggAvgIf(variants=[
        V(
            D.CLICKHOUSE, lambda expr, cond: getattr(sa.func, 'if')(
                sa.func.isNaN(sa.func.avgIf(expr, cond)),
                sa.null(),
                sa.func.avgIf(expr, cond),
            )
        ),
    ]),

    # count
    base.AggCount0.for_dialect(D.CLICKHOUSE),
    base.AggCount1.for_dialect(D.CLICKHOUSE),

    # count_if
    base.AggCountIf(variants=[
        V(D.CLICKHOUSE, lambda cond: n.func.IFNULL(sa.func.countIf(cond), 0)),
    ]),

    # countd
    base.AggCountd(variants=[
        V(D.CLICKHOUSE, sa.func.uniqExact),
    ]),

    # countd_approx
    base.AggCountdApprox(variants=[
        V(D.CLICKHOUSE, sa.func.uniq),
    ]),

    # countd_if
    base.AggCountdIf(variants=[
        V(D.CLICKHOUSE, sa.func.uniqExactIf),
    ]),

    # max
    base.AggMax.for_dialect(D.CLICKHOUSE),

    # median
    base.AggMedian(variants=[
        V(D.CLICKHOUSE, sa.func.medianExact),
    ]),

    # min
    base.AggMin.for_dialect(D.CLICKHOUSE),

    # quantile
    base.AggQuantile(variants=[
        V(D.CLICKHOUSE, lambda expr, quant: chsq_clauses.ParametrizedFunction(
            'quantileExact',
            [literal(quantile_value(un_literal(quant)), d=D.CLICKHOUSE)],
            expr)),
    ]),

    # quantile_approx
    base.AggQuantileApproximate(variants=[
        V(D.CLICKHOUSE, lambda expr, quant: chsq_clauses.ParametrizedFunction(
            'quantile',
            [literal(quantile_value(un_literal(quant)), d=D.CLICKHOUSE)],
            expr)),
    ]),

    # stdev
    base.AggStdev(variants=[
        V(D.CLICKHOUSE, sa.func.stddevSamp),
    ]),

    # stdevp
    base.AggStdevp(variants=[
        V(D.CLICKHOUSE, sa.func.stddevPop),
    ]),

    # sum
    base.AggSum.for_dialect(D.CLICKHOUSE),

    # sum_if
    base.AggSumIf(variants=[
        V(D.CLICKHOUSE, sa.func.sumIf),
    ]),

    # top_concat
    base.AggTopConcat1(variants=[
        V(D.CLICKHOUSE, _top_concat_ch),
    ]),
    base.AggTopConcat2(variants=[
        V(D.CLICKHOUSE, _top_concat_ch),
    ]),

    # var
    base.AggVar(variants=[
        V(D.CLICKHOUSE, sa.func.varSamp),
    ]),

    # varp
    base.AggVarp(variants=[
        V(D.CLICKHOUSE, sa.func.varPop),
    ]),
]
