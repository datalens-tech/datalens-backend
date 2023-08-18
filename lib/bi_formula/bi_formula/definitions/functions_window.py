from __future__ import annotations

from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseList, ClauseElement
from sqlalchemy.sql.functions import Function as SAFunction

from bi_formula.core import exc
from bi_formula.core.dialect import StandardDialect as D
from bi_formula.core.datatype import DataType
from bi_formula.definitions.type_strategy import Fixed, FromArgs
from bi_formula.definitions.args import ArgTypeSequence
from bi_formula.definitions.base import Function, TranslationVariant, WinRangeTuple
from bi_formula.definitions.literals import Literal, is_literal, un_literal
from bi_formula.definitions.common import desc


V = TranslationVariant.make

NUMERIC_TYPES = {DataType.INTEGER, DataType.FLOAT}
RANKED_TYPES = {
    DataType.BOOLEAN,
    DataType.INTEGER,
    DataType.FLOAT,
    DataType.STRING,
    DataType.DATE,
    DataType.DATETIME,
    DataType.GENERICDATETIME,
    DataType.UUID,
}
ALL_TYPES = frozenset(DataType)


class WindowFunction(Function):
    is_function = True
    is_window = True
    supports_grouping = True
    supports_bfb = True


def _order_by_from_args(*args: ClauseElement) -> ClauseList:
    """Defined the ORDER BY clause that is generated from function arguments"""
    if len(args) not in (1, 2):
        raise ValueError(f'Invalid number of arguments: {len(args)}')

    param = args[0]
    if len(args) == 2:
        assert is_literal(args[1])
        dir_value = args[1].value.lower()
    else:
        # Default ranking is from greatest to least
        dir_value = 'desc'

    if dir_value in ('asc', 'ascending'):
        pass
    elif dir_value in ('desc', 'descending'):
        param = desc(param)
    else:
        raise ValueError(dir_value)

    return ClauseList(param)


def _rows_full_window(*_: Any) -> WinRangeTuple:
    """Defines range tuple for functions that need the whole window."""
    return None, None


def _rows_stretching_window(
        _: ClauseElement, direction: ClauseElement = sa.literal('asc')
) -> WinRangeTuple:
    """Defines range tuple for functions that need a stretching or shrinking window."""
    assert is_literal(direction)
    assert isinstance(un_literal(direction), str)
    dir_value = direction.value.lower()
    if dir_value == 'asc':
        return None, 0
    elif dir_value == 'desc':
        return 0, None
    raise exc.TranslationError('Invalid value for direction parameter')


class WinGenericRankBase(WindowFunction):
    """
    Base class for all RANK* window functions.

    Some translation examples:

    RANK([Profit], "asc")  ->

        RANK() OVER (
            ORDER BY "Profit"
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )

    RANK([Profit] TOTAL)  ->

        RANK() OVER (
            ORDER BY "Profit" DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )

    RANK([Profit] WITHIN [Country])  ->

        RANK() OVER (
            PARTITION BY "Country"
            ORDER BY "Profit" DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )

    RANK([Profit] AMONG [Date])  ->

        RANK() OVER (
            PARTITION BY <all dimensions except "Date">
            ORDER BY "Profit" DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    """
    arg_names = ['value', 'direction']
    return_type = Fixed(DataType.INTEGER)


class WinRankBase(WinGenericRankBase):
    name = 'rank'
    variants = [
        V(
            D.DUMMY,
            translation=lambda *args: sa.func.RANK(),
            translation_order_by=_order_by_from_args,
            as_winfunc=True,
        ),
    ]


class WinRank1(WinRankBase):
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([RANKED_TYPES]),
    ]


class WinRank2(WinRankBase):
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([RANKED_TYPES, DataType.CONST_STRING]),
    ]


class WinRankDenseBase(WinGenericRankBase):
    name = 'rank_dense'
    variants = [
        V(
            D.DUMMY,
            translation=lambda *args: sa.func.DENSE_RANK(),
            translation_order_by=_order_by_from_args,
            as_winfunc=True,
        ),
    ]


class WinRankDense1(WinRankDenseBase):
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([RANKED_TYPES]),
    ]


class WinRankDense2(WinRankDenseBase):
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([RANKED_TYPES, DataType.CONST_STRING]),
    ]


class WinRankUniqueBase(WinGenericRankBase):
    name = 'rank_unique'
    variants = [
        V(
            D.DUMMY,
            translation=lambda *args: sa.func.ROW_NUMBER(),
            translation_order_by=_order_by_from_args,
            as_winfunc=True,
        ),
    ]


class WinRankUnique1(WinRankUniqueBase):
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([RANKED_TYPES]),
    ]


class WinRankUnique2(WinRankUniqueBase):
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([RANKED_TYPES, DataType.CONST_STRING]),
    ]


class WinRankPercentileBase(WinGenericRankBase):
    name = 'rank_percentile'
    return_type = Fixed(DataType.FLOAT)
    variants = [
        V(
            D.DUMMY,
            translation=lambda *args: sa.func.PERCENT_RANK(),
            translation_order_by=_order_by_from_args,
            as_winfunc=True,
        ),
    ]


class WinRankPercentile1(WinRankPercentileBase):
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([RANKED_TYPES]),
    ]


class WinRankPercentile2(WinRankPercentileBase):
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([RANKED_TYPES, DataType.CONST_STRING]),
    ]


class WinAggBase(WindowFunction):
    """
    Base for all window functions based on "classical" aggregation functions.

    Translation examples:

    SUM([Profit] TOTAL)  ->

        SUM("Profit") OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

    SUM([Profit] WITHIN [Date])  ->

        SUM("Profit") OVER (
            PARTITION BY "Date"
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )

    SUM([Profit] AMONG [Date])  ->

        SUM("Profit") OVER (
            PARTITION BY <all dimensions except "Date">
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    """


class WinSum(WinAggBase):
    name = 'sum'
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([NUMERIC_TYPES]),
    ]
    variants = [
        V(
            D.DUMMY,
            translation=lambda x: sa.func.SUM(x),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]
    return_type = FromArgs(0)


class WinMinMaxBase(WinAggBase):
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([RANKED_TYPES]),
    ]
    return_type = FromArgs(0)


class WinMin(WinMinMaxBase):
    name = 'min'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x: sa.func.MIN(x),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]


class WinMax(WinMinMaxBase):
    name = 'max'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x: sa.func.MAX(x),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]


class WinCountBase(WinAggBase):
    name = 'count'
    return_type = Fixed(DataType.INTEGER)


class WinCount0(WinCountBase):
    arg_cnt = 0
    variants = [
        V(
            D.DUMMY,
            translation=lambda: sa.func.COUNT(sa.text('*')),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]


class WinCount1(WinCountBase):
    arg_cnt = 1
    variants = [
        V(
            D.DUMMY,
            translation=lambda x: sa.func.COUNT(x),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]


class WinAvg(WinAggBase):  # TODO: support DATE & DATETIME
    name = 'avg'
    return_type = Fixed(DataType.FLOAT)
    arg_cnt = 1
    variants = [
        V(
            D.DUMMY,
            translation=lambda x: sa.func.AVG(x),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]


class WinAggIf(WindowFunction):
    """
    Base class for "classical" conditional window functions

    Translation examples:

    SUM_IF([Profit], [Category] = 'Office Supplies' TOTAL)  ->

        SUM("Profit") FILTER (WHERE "Category" = 'Office Supplies')
        OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

    SUM_IF([Profit], [Category] = 'Office Supplies' WITHIN [Date])  ->

        SUM("Profit") FILTER (WHERE "Category" = 'Office Supplies')
        OVER (
            PARTITION BY "Date"
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )

    SUM_IF([Profit], [Category] = 'Office Supplies' AMONG [Date])  ->
        SUM("Profit") FILTER (WHERE "Category" = 'Office Supplies')
        OVER (
            PARTITION BY <all dimensions except "Date">
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    """

    arg_cnt = 2
    arg_names = ['expression', 'condition']
    argument_types = [
        ArgTypeSequence([NUMERIC_TYPES, DataType.BOOLEAN]),
    ]


class WinSumIf(WinAggIf):
    name = 'sum_if'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, condition: sa.func.SUM(x).filter(condition),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]
    return_type = FromArgs(0)


class WinCountIf(WinAggIf):
    name = 'count_if'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, condition: sa.func.COUNT(x).filter(condition),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]
    return_type = FromArgs(0)


class WinAvgIf(WinAggIf):  # TODO: support DATE & DATETIME
    name = 'avg_if'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, condition: sa.func.AVG(x).filter(condition),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]
    return_type = FromArgs(0)


class OrderedWinFuncBase(WindowFunction):
    uses_default_ordering = True
    supports_ordering = True


class WinRFuncBase(OrderedWinFuncBase):
    """
    Base class for cumulative window functions.

    Translation examples:

    RSUM([Profit])  ->

        SUM("Profit") OVER (
            ORDER BY <same as query's main ORDER BY clause>
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )

    RSUM([Profit], "desc" WITHIN [Date])  ->

        SUM("Profit") OVER (
            PARTITION BY "Date"
            ORDER BY <same as query's main ORDER BY clause>
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )

     RSUM([Profit] AMONG [Date])  ->

        SUM("Profit") OVER (
            PARTITION BY <all dimensions except "Date">
            ORDER BY <same as query's main ORDER BY clause>
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    """
    arg_names = ['value', 'direction']


class WinRSumBase(WinRFuncBase):
    name = 'rsum'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, *_: sa.func.sum(x),
            translation_rows=_rows_stretching_window,
            as_winfunc=True,
        ),
    ]
    argument_types = [
        ArgTypeSequence([NUMERIC_TYPES, DataType.CONST_STRING]),
    ]
    return_type = FromArgs(0)


class WinRSum1(WinRSumBase):
    arg_cnt = 1


class WinRSum2(WinRSumBase):
    arg_cnt = 2


class WinRCountBase(WinRFuncBase):
    name = 'rcount'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, *_: sa.func.COUNT(x),
            translation_rows=_rows_stretching_window,
            as_winfunc=True,
        ),
    ]
    argument_types = [
        ArgTypeSequence([ALL_TYPES, DataType.CONST_STRING]),
    ]
    return_type = Fixed(DataType.INTEGER)


class WinRCount1(WinRCountBase):
    arg_cnt = 1


class WinRCount2(WinRCountBase):
    arg_cnt = 2


class WinRMinMaxBase(WinRFuncBase):
    argument_types = [
        ArgTypeSequence([RANKED_TYPES, DataType.CONST_STRING]),
    ]
    return_type = FromArgs(0)


class WinRMinBase(WinRMinMaxBase):
    name = 'rmin'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, *_: sa.func.MIN(x),
            translation_rows=_rows_stretching_window,
            as_winfunc=True,
        ),
    ]


class WinRMin1(WinRMinBase):
    arg_cnt = 1


class WinRMin2(WinRMinBase):
    arg_cnt = 2


class WinRMaxBase(WinRMinMaxBase):
    name = 'rmax'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, *_: sa.func.MAX(x),
            translation_rows=_rows_stretching_window,
            as_winfunc=True,
        ),
    ]


class WinRMax1(WinRMaxBase):
    arg_cnt = 1


class WinRMax2(WinRMaxBase):
    arg_cnt = 2


class WinRAvgBase(WinRFuncBase):  # TODO: Add support for DATE & DATETIME
    name = 'ravg'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, *_: sa.func.AVG(x),
            translation_rows=_rows_stretching_window,
            as_winfunc=True,
        ),
    ]
    argument_types = [
        ArgTypeSequence([NUMERIC_TYPES, DataType.CONST_STRING]),
    ]
    return_type = Fixed(DataType.INTEGER)


class WinRAvg1(WinRAvgBase):
    arg_cnt = 1


class WinRAvg2(WinRAvgBase):
    arg_cnt = 2


def _resolve_mfunc_rows(raw_arg1: Literal, raw_arg2: Optional[Literal] = None) -> WinRangeTuple:
    """
    Create range tuple for the following cases:
    ::

        MSUM(SUM([Sales]), 5)

    Calculates the sum of the current row and the previous 5
    ::

        MSUM(SUM([Sales]), -5)

    Calculates the sum of the current row and the following 5
    ::

        MSUM(SUM([Sales]), 3, 8)

    Calculates the sum of the previous 3, the current row and the following 8
    """
    arg1 = raw_arg1.value
    arg2 = raw_arg2.value if raw_arg2 is not None else None

    if arg2 is not None:
        return (-abs(arg1), abs(arg2))
    if arg1 >= 0:
        return (-arg1, 0)
    return (0, abs(arg1))


class MFuncBase(OrderedWinFuncBase):
    """
    Base class for moving window functions

    MSUM([Profit], -2)  ->

        SUM("Profit") OVER (
            ORDER BY <same as query's main ORDER BY clause>
            ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING
        )

    MSUM([Profit], 3)  ->

        SUM("Profit") OVER (
            ORDER BY <same as query's main ORDER BY clause>
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        )

    MSUM([Profit] 5, 5 TOTAL)  ->

        SUM("Profit") OVER (
            ORDER BY <same as query's main ORDER BY clause>
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        )

    MSUM([Profit], -5 WITHIN [Date])  ->

        SUM("Profit") OVER (
            PARTITION BY "Date"
            ORDER BY <same as query's main ORDER BY clause>
            ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING
        )

    MSUM([Profit], -5 AMONG [Date])  ->

        SUM("Profit") OVER (
            PARTITION BY <all dimensions except "Date">
            ORDER BY <same as query's main ORDER BY clause>
            ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING
        )
    """
    arg_names = ['value', 'rows_1', 'rows_2']


class WinMSumBase(MFuncBase):
    name = 'msum'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, *_: sa.func.sum(x),
            translation_rows=lambda _, *row_args: _resolve_mfunc_rows(*row_args),
            as_winfunc=True,
        ),
    ]
    argument_types = [
        ArgTypeSequence([NUMERIC_TYPES, DataType.CONST_INTEGER, DataType.CONST_INTEGER]),
    ]
    return_type = FromArgs(0)


class WinMSum2(WinMSumBase):
    arg_cnt = 2


class WinMSum3(WinMSumBase):
    arg_cnt = 3


class WinMCountBase(MFuncBase):
    name = 'mcount'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, *_: sa.func.COUNT(x),
            translation_rows=lambda _, *row_args: _resolve_mfunc_rows(*row_args),
            as_winfunc=True,
        ),
    ]
    argument_types = [
        ArgTypeSequence([ALL_TYPES, DataType.CONST_INTEGER, DataType.CONST_INTEGER]),
    ]
    return_type = FromArgs(0)


class WinMCount2(WinMCountBase):
    arg_cnt = 2


class WinMCount3(WinMCountBase):
    arg_cnt = 3


class WinMMinMaxBase(MFuncBase):
    argument_types = [
        ArgTypeSequence([RANKED_TYPES, DataType.CONST_INTEGER, DataType.CONST_INTEGER]),
    ]
    return_type = FromArgs(0)


class WinMMinBase(WinMMinMaxBase):
    name = 'mmin'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, *_: sa.func.MIN(x),
            translation_rows=lambda _, *row_args: _resolve_mfunc_rows(*row_args),
            as_winfunc=True,
        ),
    ]


class WinMMin2(WinMMinBase):
    arg_cnt = 2


class WinMMin3(WinMMinBase):
    arg_cnt = 3


class WinMMaxBase(WinMMinMaxBase):
    name = 'mmax'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, *_: sa.func.MAX(x),
            translation_rows=lambda _, *row_args: _resolve_mfunc_rows(*row_args),
            as_winfunc=True,
        ),
    ]


class WinMMax2(WinMMaxBase):
    arg_cnt = 2


class WinMMax3(WinMMaxBase):
    arg_cnt = 3


class WinMAvgBase(MFuncBase):  # TODO: support DATE & DATETIME
    name = 'mavg'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x, *_: sa.func.AVG(x),
            translation_rows=lambda _, *row_args: _resolve_mfunc_rows(*row_args),
            as_winfunc=True,
        ),
    ]
    argument_types = [
        ArgTypeSequence([NUMERIC_TYPES, DataType.CONST_INTEGER, DataType.CONST_INTEGER]),
    ]
    return_type = FromArgs(0)


class WinMAvg2(WinMAvgBase):
    arg_cnt = 2


class WinMAvg3(WinMAvgBase):
    arg_cnt = 3


def lag_implementation(
        x: Any, offset: Any = sa.literal(1), default: Any = sa.null(),
        lag_name: str = 'LAG', lead_name: str = 'LEAD',
) -> ClauseElement:
    result: ClauseElement
    if un_literal(offset) > 0:
        result = SAFunction(lag_name, x, un_literal(offset), un_literal(default))
    elif un_literal(offset) == 0:
        result = x
    else:
        result = SAFunction(lead_name, x, sa.literal(-un_literal(offset)), un_literal(default))
    return result


class WinLagBase(OrderedWinFuncBase):
    name = 'lag'
    arg_names = ['value', 'offset', 'default']
    variants = [
        V(
            D.DUMMY,
            # PostgreSQL requires offset and default to be constant,
            # so we must unwrap these values in case casts have been added to them.
            translation=lambda x, offset=sa.literal(1), default=sa.null(), *_: (
                lag_implementation(x, offset=offset, default=default)
            ),
            translation_rows=lambda x, offset=sa.literal(1), *_: (None, None),
            as_winfunc=True,
        ),
    ]
    argument_types = [
        ArgTypeSequence([
            set(DataType),
            DataType.CONST_INTEGER,
            {dtype for dtype in DataType if dtype.is_const}
        ]),
    ]
    return_type = FromArgs(0)


class WinLag1(WinLagBase):
    arg_cnt = 1


class WinLag2(WinLagBase):
    arg_cnt = 2


class WinLag3(WinLagBase):
    arg_cnt = 3


class WinFirstLastBase(OrderedWinFuncBase):
    arg_cnt = 1
    arg_names = ['value']
    argument_types = [
        ArgTypeSequence([
            set(DataType),
        ]),
    ]
    return_type = FromArgs(0)


class WinFirst(WinFirstLastBase):
    name = 'first'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x: sa.func.first_value(x),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]


class WinLast(WinFirstLastBase):
    name = 'last'
    variants = [
        V(
            D.DUMMY,
            translation=lambda x: sa.func.last_value(x),
            translation_rows=_rows_full_window,
            as_winfunc=True,
        ),
    ]


DEFINITIONS_WINDOW = [
    # avg
    WinAvg,

    # avg_if
    WinAvgIf,

    # count
    WinCount0,
    WinCount1,

    # count_if
    WinCountIf,

    # first
    WinFirst,

    # lag
    WinLag1,
    WinLag2,
    WinLag3,

    # last
    WinLast,

    # mavg
    WinMAvg2,
    WinMAvg3,

    # max
    WinMax,

    # mcount
    WinMCount2,
    WinMCount3,

    # min
    WinMin,

    # mmax
    WinMMax2,
    WinMMax3,

    # mmin
    WinMMin2,
    WinMMin3,

    # msum
    WinMSum2,
    WinMSum3,

    # rank
    WinRank1,
    WinRank2,

    # rank_dense
    WinRankDense1,
    WinRankDense2,

    # rank_percentile
    WinRankPercentile1,
    WinRankPercentile2,

    # rank_unique
    WinRankUnique1,
    WinRankUnique2,

    # ravg
    WinRAvg1,
    WinRAvg2,

    # rcount
    WinRCount1,
    WinRCount2,

    # rmax
    WinRMax1,
    WinRMax2,

    # rmin
    WinRMin1,
    WinRMin2,

    # rsum
    WinRSum1,
    WinRSum2,

    # sum
    WinSum,

    # sum_if
    WinSumIf,
]
