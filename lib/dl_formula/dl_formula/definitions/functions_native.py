import re

from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.functions import Function as SqlFunction

from dl_formula.core import exc
from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.core.nodes import LiteralString
from dl_formula.definitions.args import ArgTypeSequenceThenForAll
from dl_formula.definitions.base import (
    Function,
    TranslationVariantWrapped,
)
from dl_formula.definitions.functions_aggregation import AggregationFunctionBase
from dl_formula.definitions.type_strategy import Fixed
from dl_formula.translation.context import TranslationCtx


VW = TranslationVariantWrapped.make


def _call_native_impl(func_name_ctx: TranslationCtx, *args: TranslationCtx) -> ClauseElement:
    assert isinstance(func_name_ctx.node, LiteralString)
    func_name = func_name_ctx.node.value

    # Validate function name
    if not re.match(r"^[a-zA-Z0-9_]+$", func_name):
        raise exc.NativeFunctionForbiddenInputError(func_name)

    return SqlFunction(func_name, *(arg.expression for arg in args))


class DBCall(Function):
    arg_cnt = None
    arg_names = ["db_function_name"]
    argument_types = [
        ArgTypeSequenceThenForAll(fixed_arg_types=[DataType.CONST_STRING], for_all_types=set(DataType)),
    ]
    variants = [VW(D.DUMMY, _call_native_impl)]


class DBCallInt(DBCall):
    name = "db_call_int"
    return_type = Fixed(DataType.INTEGER)


class DBCallFloat(DBCall):
    name = "db_call_float"
    return_type = Fixed(DataType.FLOAT)


class DBCallString(DBCall):
    name = "db_call_string"
    return_type = Fixed(DataType.STRING)


class DBCallBool(DBCall):
    name = "db_call_bool"
    return_type = Fixed(DataType.BOOLEAN)


class DBCallArrayInt(DBCall):
    name = "db_call_array_int"
    return_type = Fixed(DataType.ARRAY_INT)


class DBCallArrayFloat(DBCall):
    name = "db_call_array_float"
    return_type = Fixed(DataType.ARRAY_FLOAT)


class DBCallArrayString(DBCall):
    name = "db_call_array_string"
    return_type = Fixed(DataType.ARRAY_STR)


class DBCallAgg(AggregationFunctionBase):
    arg_cnt = None
    arg_names = ["db_agg_function_name"]
    argument_types = [
        ArgTypeSequenceThenForAll(fixed_arg_types=[DataType.CONST_STRING], for_all_types=set(DataType)),
    ]
    variants = [VW(D.DUMMY, _call_native_impl)]


class DBCallAggInt(DBCallAgg):
    name = "db_call_agg_int"
    return_type = Fixed(DataType.INTEGER)


class DBCallAggFloat(DBCallAgg):
    name = "db_call_agg_float"
    return_type = Fixed(DataType.FLOAT)


class DBCallAggString(DBCallAgg):
    name = "db_call_agg_string"
    return_type = Fixed(DataType.STRING)


DEFINITIONS_NATIVE = [
    DBCallInt,
    DBCallFloat,
    DBCallString,
    DBCallBool,
    DBCallArrayInt,
    DBCallArrayFloat,
    DBCallArrayString,
    DBCallAggInt,
    DBCallAggFloat,
    DBCallAggString,
]
