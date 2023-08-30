from __future__ import annotations

import bi_formula.core.exc as exc
import bi_formula.core.aux_nodes as aux_nodes
from bi_formula.core.dialect import StandardDialect as D
from bi_formula.core.datatype import DataType
from bi_formula.definitions.args import ArgTypeSequence
from bi_formula.definitions.type_strategy import FromArgs
from bi_formula.definitions.base import (
    TranslationVariantWrapped,
    Function,
)


VW = TranslationVariantWrapped.make


class TimeSeriesFunction(Function):
    supports_ignore_dimensions = True
    supports_bfb = True
    variants = [
        # These functions are not translatable,
        # so raise an error if we ever reach the translator here.
        VW(D.ANY, lambda *args: aux_nodes.ErrorNode.make(
            message='Lookup function should have been mutated',
            err_code=exc.TranslationError.default_code,
        ))
    ]


class FuncAgoBase(TimeSeriesFunction):
    name = 'ago'
    arg_names = ['measure', 'date_dimension', 'unit', 'number']
    argument_types = [
        ArgTypeSequence([
            set(DataType),
            {DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME},
            DataType.CONST_STRING,
            DataType.CONST_INTEGER,
        ]),
    ]
    return_type = FromArgs(0)


class FuncAgo2(FuncAgoBase):
    arg_cnt = 2


class FuncAgo3Unit(FuncAgoBase):
    arg_cnt = 3


class FuncAgo3Number(FuncAgoBase):
    """A variant with the number of days as the third argument with the units omitted"""
    arg_cnt = 3
    argument_types = [
        ArgTypeSequence([
            set(DataType),
            {DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME},
            DataType.CONST_INTEGER,
        ]),
    ]


class FuncAgo4(FuncAgoBase):
    arg_cnt = 4


class FuncAtDateBase(TimeSeriesFunction):
    name = 'at_date'
    arg_names = ['measure', 'date_dimension', 'date_expr']
    argument_types = [
        ArgTypeSequence([set(DataType), DataType.DATE, DataType.DATE]),
        ArgTypeSequence([set(DataType), DataType.DATETIME, DataType.DATETIME]),
        ArgTypeSequence([set(DataType), DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
    ]
    return_type = FromArgs(0)


class FuncAtDate3(FuncAtDateBase):
    arg_cnt = 3


DEFINITIONS_TIME_SERIES = [
    # ago
    FuncAgo2,
    FuncAgo3Unit,
    FuncAgo3Number,
    FuncAgo4,
    # at_date
    FuncAtDate3,
]
