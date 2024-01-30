import sqlalchemy as sa
import ydb.sqlalchemy as ydb_sa

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.common import quantile_value
import dl_formula.definitions.functions_aggregation as base
from dl_formula.definitions.literals import un_literal
from dl_formula.shortcuts import n

from dl_connector_ydb.formula.constants import YqlDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


def _all_concat_yql(expr, sep=", "):
    res = expr
    res = sa.cast(res, sa.TEXT)
    res = sa.func.AGGREGATE_LIST_DISTINCT(res)
    res = sa.func.ListSortAsc(res)
    # Would be nicer to cast List<Whaverer> to List<Utf8> at this point.
    res = sa.func.String.JoinFromList(res, sep)
    return res


DEFINITIONS_AGG = [
    # all_concat
    base.AggAllConcat1(
        variants=[
            V(D.YQL, _all_concat_yql),
        ]
    ),
    base.AggAllConcat2(
        variants=[
            V(D.YQL, _all_concat_yql),
        ]
    ),
    # any
    base.AggAny(
        variants=[
            V(D.YQL, sa.func.SOME),
        ]
    ),
    # arg_max
    base.AggArgMax(
        variants=[
            V(D.YQL, sa.func.MAX_BY),
        ]
    ),
    # arg_min
    base.AggArgMin(
        variants=[
            V(D.YQL, sa.func.MIN_BY),
        ]
    ),
    # avg
    base.AggAvgFromNumber.for_dialect(D.YQL),
    base.AggAvgFromDate(
        variants=[
            # in YQL AVG returns Double which can not be casted to Datetime so we have to convert it to INT explicitly
            VW(D.YQL, lambda date_val: n.func.DATE(n.func.INT(n.func.AVG(n.func.INT(date_val)))))
        ]
    ),
    base.AggAvgFromDatetime.for_dialect(D.YQL),
    # avg_if
    base.AggAvgIf(
        variants=[
            V(D.YQL, sa.func.avg_if),
        ]
    ),
    # count
    base.AggCount0.for_dialect(D.YQL),
    base.AggCount1.for_dialect(D.YQL),
    # count_if
    base.AggCountIf(
        variants=[
            V(D.YQL, sa.func.COUNT_IF),
        ]
    ),
    # countd
    base.AggCountd.for_dialect(D.YQL),
    # countd_approx
    base.AggCountdApprox(
        variants=[
            V(D.YQL, sa.func.CountDistinctEstimate),
        ]
    ),
    # countd_if
    base.AggCountdIf.for_dialect(D.YQL),
    # max
    base.AggMax.for_dialect(D.YQL),
    # median
    base.AggMedian(
        variants=[
            V(D.YQL, sa.func.MEDIAN),
        ]
    ),
    # min
    base.AggMin.for_dialect(D.YQL),
    # quantile
    base.AggQuantile(
        variants=[
            V(
                D.YQL,
                lambda expr, quant: sa.func.PERCENTILE(
                    expr,
                    quantile_value(un_literal(quant)),
                ),
            ),
        ]
    ),
    # stdev
    base.AggStdev(
        variants=[
            V(D.YQL, sa.func.STDDEVSAMP),
        ]
    ),
    # stdevp
    base.AggStdevp(
        variants=[
            V(D.YQL, sa.func.STDDEVPOP),
        ]
    ),
    # sum
    base.AggSum.for_dialect(D.YQL),
    # sum_if
    base.AggSumIf(
        variants=[
            V(D.YQL, sa.func.SUM_IF),
        ]
    ),
    # top_concat
    base.AggTopConcat1(
        variants=[
            # String::JoinFromList(ListMap(TOPFREQ(expr, amount), ($x) -> { RETURN cast($x.Value as Utf8); }), sep)
            V(
                D.YQL,
                lambda expr, amount, sep=", ": sa.func.String.JoinFromList(
                    sa.func.ListMap(sa.func.TOPFREQ(expr, amount), ydb_sa.types.Lambda(lambda x: sa.cast(x, sa.Text))),
                    ", ",
                ),
            ),
        ]
    ),
    base.AggTopConcat2(
        variants=[
            # String::JoinFromList(ListMap(TOPFREQ(expr, amount), ($x) -> { RETURN cast($x.Value as Utf8); }), sep)
            V(
                D.YQL,
                lambda expr, amount, sep=", ": sa.func.String.JoinFromList(
                    sa.func.ListMap(sa.func.TOPFREQ(expr, amount), ydb_sa.types.Lambda(lambda x: sa.cast(x, sa.Text))),
                    ", ",
                ),
            ),
        ]
    ),
    # var
    base.AggVar(
        variants=[
            V(D.YQL, sa.func.VARSAMP),
        ]
    ),
    # varp
    base.AggVarp(
        variants=[
            V(D.YQL, sa.func.VARPOP),
        ]
    ),
]
