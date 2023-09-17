from __future__ import annotations

import sqlalchemy as sa

from bi_formula.core.datatype import DataType
from bi_formula.core.dialect import StandardDialect as D
from bi_formula.definitions.args import (
    ArgFlagSequence,
    ArgTypeSequence,
)
from bi_formula.definitions.base import (
    Function,
    TranslationVariant,
    TranslationVariantWrapped,
)
from bi_formula.definitions.flags import ContextFlag
from bi_formula.definitions.type_strategy import (
    Fixed,
    FromArgs,
    ParamsFromArgs,
)
from bi_formula.shortcuts import n

V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class AggregationFunction(Function):
    is_aggregation = True
    supports_lod = True
    supports_bfb = True
    arg_cnt = 1


class AggSum(AggregationFunction):
    name = "sum"
    variants = [
        V(D.DUMMY | D.SQLITE, sa.func.sum),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = FromArgs()


class AggSumIf(AggregationFunction):
    name = "sum_if"
    variants = [
        VW(
            D.DUMMY,
            lambda expr, cond: n.func.SUM(n.func.IF(cond, expr, 0)),
        ),
    ]
    arg_cnt = 2
    arg_names = ["expression", "condition"]
    # TODO: Check if really all types for conditions are allowed
    # TODO: Check for case: bi-formula-cli translate --dialect ORACLE "SUM_IF([int_value], [int_value])"
    # argument_types = [
    #     *[[DataType.INTEGER, cond_type] for cond_type in DataType],
    #     *[[DataType.FLOAT, cond_type] for cond_type in DataType],
    # ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.BOOLEAN]),
        ArgTypeSequence([DataType.FLOAT, DataType.BOOLEAN]),
    ]
    argument_flags = ArgFlagSequence([None, ContextFlag.REQ_CONDITION])
    return_type = FromArgs(0)


class AggAvg(AggregationFunction):
    name = "avg"
    arg_names = ["value"]


class AggAvgFromNumber(AggAvg):
    variants = [
        V(D.DUMMY | D.SQLITE, sa.func.avg),
    ]
    return_type = Fixed(DataType.FLOAT)
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]


class AggAvgFromDate(AggAvg):
    variants = [
        VW(
            D.DUMMY,
            lambda date_val: n.func.DATE(n.func.AVG(n.func.INT(date_val))),
        ),
    ]
    return_type = Fixed(DataType.DATE)
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
    ]


class AggAvgFromDatetime(AggAvg):
    variants = [
        VW(
            D.DUMMY,
            lambda dt_val: n.func.DATETIME(n.func.AVG(n.func.INT(dt_val))),
        ),
    ]
    return_type = FromArgs(0)
    argument_types = [
        ArgTypeSequence([DataType.DATETIME]),
        ArgTypeSequence([DataType.GENERICDATETIME]),
    ]


class AggAvgFromDatetimeTZ(AggAvg):
    variants = [
        VW(
            D.DUMMY,
            lambda dt: n.func.DATETIMETZ(n.func.AVG(n.func.INT(dt)), dt.data_type_params.timezone),
        ),
    ]
    argument_types = [ArgTypeSequence([DataType.DATETIMETZ])]
    return_type = FromArgs(0)
    return_type_params = ParamsFromArgs(0)


class AggAvgIf(AggregationFunction):
    name = "avg_if"
    variants = [
        VW(
            D.DUMMY,
            lambda expr, cond: n.func.AVG(n.func.IF(cond, expr, None)),
        ),
    ]

    arg_cnt = 2
    arg_names = ["expression", "condition"]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.BOOLEAN]),
        ArgTypeSequence([DataType.FLOAT, DataType.BOOLEAN]),
    ]
    argument_flags = ArgFlagSequence([None, ContextFlag.REQ_CONDITION])
    return_type = Fixed(DataType.FLOAT)


class AggMax(AggregationFunction):
    name = "max"
    arg_names = ["value"]
    variants = [
        V(D.DUMMY | D.SQLITE, sa.func.max),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
        ArgTypeSequence([DataType.BOOLEAN]),
        ArgTypeSequence([DataType.STRING]),
        ArgTypeSequence([DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME]),
        ArgTypeSequence([DataType.DATETIMETZ]),
        ArgTypeSequence([DataType.GENERICDATETIME]),
        ArgTypeSequence([DataType.UUID]),
        ArgTypeSequence([DataType.INTEGER]),
    ]
    return_type = FromArgs(0)
    return_type_params = ParamsFromArgs(0)


class AggMin(AggregationFunction):
    name = "min"
    arg_names = ["value"]
    variants = [
        V(D.DUMMY | D.SQLITE, sa.func.min),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
        ArgTypeSequence([DataType.BOOLEAN]),
        ArgTypeSequence([DataType.STRING]),
        ArgTypeSequence([DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME]),
        ArgTypeSequence([DataType.DATETIMETZ]),
        ArgTypeSequence([DataType.GENERICDATETIME]),
        ArgTypeSequence([DataType.UUID]),
        ArgTypeSequence([DataType.INTEGER]),
    ]
    return_type = FromArgs(0)
    return_type_params = ParamsFromArgs(0)


class AggCount(AggregationFunction):
    name = "count"
    arg_names = ["value"]
    return_type = Fixed(DataType.INTEGER)


class AggCount0(AggCount):
    arg_cnt = 0
    variants = [
        V(D.DUMMY | D.SQLITE, lambda: sa.func.count(1)),
    ]


class AggCount1(AggCount):
    variants = [
        V(D.DUMMY | D.SQLITE, sa.func.count),
    ]


class AggCountIf(AggregationFunction):
    name = "count_if"
    variants = [
        VW(
            D.DUMMY,
            lambda cond: n.func.COUNT(n.func.IF(cond, 1, None)),
        )
    ]

    arg_names = ["condition"]
    argument_types = [
        ArgTypeSequence([DataType.BOOLEAN]),
    ]
    argument_flags = ArgFlagSequence([ContextFlag.REQ_CONDITION])
    return_type = Fixed(DataType.INTEGER)


class AggCountd(AggregationFunction):
    name = "countd"
    variants = [
        V(D.DUMMY | D.SQLITE, lambda x: sa.func.count(sa.distinct(x))),
    ]
    return_type = Fixed(DataType.INTEGER)


class AggCountdIf(AggregationFunction):
    name = "countd_if"
    variants = [
        VW(
            D.DUMMY,
            lambda expr, cond: n.func.COUNTD(n.func.IF(cond, expr, None)),
        ),
    ]

    arg_cnt = 2
    arg_names = ["expression", "condition"]
    argument_types = [ArgTypeSequence([expr_type, DataType.BOOLEAN]) for expr_type in DataType]
    argument_flags = ArgFlagSequence([None, ContextFlag.REQ_CONDITION])
    return_type = Fixed(DataType.INTEGER)


class AggCountdApprox(AggregationFunction):
    name = "countd_approx"
    return_type = Fixed(DataType.INTEGER)


class AggStdev(AggregationFunction):
    name = "stdev"
    variants = [
        V(D.DUMMY, sa.func.STDDEV_SAMP),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class AggStdevp(AggregationFunction):
    name = "stdevp"
    variants = [
        V(D.DUMMY, sa.func.STDDEV_POP),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class AggVar(AggregationFunction):
    name = "var"
    variants = [
        V(D.DUMMY, sa.func.VAR_SAMP),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class AggVarp(AggregationFunction):
    name = "varp"
    variants = [
        V(D.DUMMY, sa.func.VAR_POP),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class AggQuantileBase(AggregationFunction):
    arg_cnt = 2
    arg_names = ["value", "quant"]
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.INTEGER, DataType.FLOAT, DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME},
                DataType.FLOAT,
            ]
        ),
    ]
    return_type = FromArgs(0)


class AggQuantile(AggQuantileBase):
    name = "quantile"


class AggQuantileApproximate(AggQuantileBase):
    name = "quantile_approx"


class AggMedian(AggregationFunction):
    name = "median"
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.INTEGER, DataType.FLOAT, DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME},
            ]
        ),
    ]
    return_type = FromArgs()


class AggAttr(AggregationFunction):
    name = "attr"
    # TODO: Implement me


class AggAny(AggregationFunction):
    name = "any"
    return_type = FromArgs()


class AggArgMinMax(AggregationFunction):
    return_type = FromArgs(0)


class AggArgMin(AggArgMinMax):
    name = "arg_min"
    arg_cnt = 2
    arg_names = ["value", "comp"]


class AggArgMax(AggArgMinMax):
    name = "arg_max"
    arg_cnt = 2
    arg_names = ["value", "comp"]


# Blacklist of types,
# because most types should get stringified and concatenated successfully,
# but markup, while internally a string, shouldn't become a string
# (and requires a bit more specific concatenation).
# TODO: support markup agg-concat.
# `DataType.UNSUPPORTED` could also work, but the idea is that the user should
# add `STR()` explicitly for those.
AGG_CONCAT_INPUT_TYPES = set(DataType) - set((DataType.MARKUP, DataType.CONST_MARKUP, DataType.UNSUPPORTED))


class AggAllConcatBase(AggregationFunction):
    name = "all_concat"
    arg_names = ["expression", "separator"]
    # (Any, ConstString) -> String
    argument_types = [
        ArgTypeSequence(
            [
                AGG_CONCAT_INPUT_TYPES,
                DataType.CONST_STRING,
            ]
        ),
    ]
    return_type = Fixed(DataType.STRING)


class AggAllConcat1(AggAllConcatBase):
    arg_cnt = 1


class AggAllConcat2(AggAllConcatBase):
    arg_cnt = 2


class AggTopConcatBase(AggregationFunction):
    name = "top_concat"
    # Doable for postgresql, but not performant, and very convoluted
    arg_names = ["expression", "amount", "separator"]
    # (Any, ConstInteger, ConstString) -> String
    argument_types = [
        ArgTypeSequence(
            [
                AGG_CONCAT_INPUT_TYPES,
                DataType.CONST_INTEGER,
                DataType.CONST_STRING,
            ]
        ),
    ]
    return_type = Fixed(DataType.STRING)


class AggTopConcat1(AggTopConcatBase):
    arg_cnt = 2


class AggTopConcat2(AggTopConcatBase):
    arg_cnt = 3


DEFINITIONS_AGGREGATION = [
    # all_concat
    AggAllConcat1,
    AggAllConcat2,
    # any
    AggAny,
    # arg_max
    AggArgMax,
    # arg_min
    AggArgMin,
    # avg
    AggAvgFromNumber,
    AggAvgFromDate,
    AggAvgFromDatetime,
    AggAvgFromDatetimeTZ,
    # avg_if
    AggAvgIf,
    # count
    AggCount0,
    AggCount1,
    # count_if
    AggCountIf,
    # countd
    AggCountd,
    # countd_approx
    AggCountdApprox,
    # countd_if
    AggCountdIf,
    # max
    AggMax,
    # median
    AggMedian,
    # min
    AggMin,
    # quantile
    AggQuantile,
    # quantile_approx
    AggQuantileApproximate,
    # stdev
    AggStdev,
    # stdevp
    AggStdevp,
    # sum
    AggSum,
    # sum_if
    AggSumIf,
    # top_concat
    AggTopConcat1,
    AggTopConcat2,
    # var
    AggVar,
    # varp
    AggVarp,
]
