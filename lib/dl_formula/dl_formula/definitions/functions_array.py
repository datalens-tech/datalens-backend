from __future__ import annotations

from typing import (
    List,
    Type,
)

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import StandardDialect as D
import dl_formula.core.nodes as nodes
from dl_formula.definitions.args import (
    ArgTypeForAll,
    ArgTypeSequence,
)
from dl_formula.definitions.base import (
    Function,
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.flags import ContextFlag
from dl_formula.definitions.functions_string import (
    FuncContains,
    FuncLen,
    FuncNotContains,
    FuncStartswith,
)
from dl_formula.definitions.literals import un_literal
from dl_formula.definitions.type_strategy import (
    Fixed,
    FromArgs,
)
from dl_formula.shortcuts import n
from dl_formula.translation.context import TranslationCtx


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class ArrayFunction(Function):
    pass


class FuncCreateArray(ArrayFunction):
    name = "array"
    arg_names = ["value_1", "value_2", "value_3"]
    arg_cnt = None


def _normalize_arr_items(val: List[TranslationCtx], item_type: Type) -> list:
    """Unliteralize and convert array items"""
    result = []
    for item in val:
        un_lit_item = un_literal(item.expression)
        if un_lit_item is not None:
            un_lit_item = item_type(un_lit_item)
        result.append(un_lit_item)
    return result


class FuncConstArrayBase(FuncCreateArray):
    pass


class FuncConstArrayFloat(FuncConstArrayBase):
    variants = [
        VW(D.DUMMY, lambda *args: nodes.LiteralArrayFloat.make(_normalize_arr_items(args, float))),
    ]
    argument_types = [
        ArgTypeForAll({DataType.CONST_FLOAT, DataType.NULL}, require_type_match=DataType.CONST_FLOAT),
    ]
    return_type = Fixed(DataType.CONST_ARRAY_FLOAT)


class FuncConstArrayInt(FuncConstArrayBase):
    variants = [
        VW(D.DUMMY, lambda *args: nodes.LiteralArrayInteger.make(_normalize_arr_items(args, int))),
    ]
    argument_types = [
        ArgTypeForAll({DataType.CONST_INTEGER, DataType.NULL}, require_type_match=DataType.CONST_INTEGER),
    ]
    return_type = Fixed(DataType.CONST_ARRAY_INT)


class FuncConstArrayStr(FuncConstArrayBase):
    variants = [
        VW(D.DUMMY, lambda *args: nodes.LiteralArrayString.make(_normalize_arr_items(args, str))),
    ]
    argument_types = [
        ArgTypeForAll({DataType.CONST_STRING, DataType.NULL}, require_type_match=DataType.CONST_STRING),
    ]
    return_type = Fixed(DataType.CONST_ARRAY_STR)


class FuncNonConstArrayBase(FuncCreateArray):
    pass


class FuncNonConstArrayInt(FuncNonConstArrayBase):
    argument_types = [
        ArgTypeForAll(DataType.INTEGER, require_type_match={DataType.INTEGER, DataType.CONST_INTEGER}),
    ]
    return_type = Fixed(DataType.ARRAY_INT)


class FuncNonConstArrayFloat(FuncNonConstArrayBase):
    argument_types = [
        ArgTypeForAll(DataType.FLOAT, require_type_match={DataType.FLOAT, DataType.CONST_FLOAT}),
    ]
    return_type = Fixed(DataType.ARRAY_FLOAT)


class FuncNonConstArrayStr(FuncNonConstArrayBase):
    argument_types = [
        ArgTypeForAll(DataType.STRING, require_type_match={DataType.STRING, DataType.CONST_STRING}),
    ]
    return_type = Fixed(DataType.ARRAY_STR)


class FuncUnnestArray(ArrayFunction):
    name = "unnest"
    arg_names = ["array"]
    arg_cnt = 1


class FuncUnnestArrayFloat(FuncUnnestArray):
    argument_types = [ArgTypeSequence([DataType.ARRAY_FLOAT])]
    return_type = Fixed(DataType.FLOAT)


class FuncUnnestArrayInt(FuncUnnestArray):
    argument_types = [ArgTypeSequence([DataType.ARRAY_INT])]
    return_type = Fixed(DataType.INTEGER)


class FuncUnnestArrayStr(FuncUnnestArray):
    argument_types = [ArgTypeSequence([DataType.ARRAY_STR])]
    return_type = Fixed(DataType.STRING)


class FuncLenArray(FuncLen):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
        ArgTypeSequence([DataType.ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_STR]),
    ]


class FuncGetArrayItem(ArrayFunction):
    name = "get_item"
    arg_names = ["array", "index"]
    arg_cnt = 2


class FuncGetArrayItemFloat(FuncGetArrayItem):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncGetArrayItemInt(FuncGetArrayItem):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.INTEGER)


class FuncGetArrayItemStr(FuncGetArrayItem):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_STR, DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncArrStr(ArrayFunction):
    name = "arr_str"
    arg_names = ["array", "delimiter", "null_str"]
    return_type = Fixed(DataType.STRING)


class FuncArrStr1(FuncArrStr):
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
        ArgTypeSequence([DataType.ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_STR]),
    ]


class FuncArrStr2(FuncArrStr):
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.CONST_STRING]),
        ArgTypeSequence([DataType.ARRAY_INT, DataType.CONST_STRING]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.CONST_STRING]),
    ]


class FuncArrStr3(FuncArrStr):
    arg_cnt = 3
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.CONST_STRING, DataType.CONST_STRING]),
        ArgTypeSequence([DataType.ARRAY_INT, DataType.CONST_STRING, DataType.CONST_STRING]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.CONST_STRING, DataType.CONST_STRING]),
    ]


class FuncArrayCountItem(ArrayFunction):
    name = "count_item"
    arg_names = ["array", "value"]
    arg_cnt = 2

    return_type = Fixed(DataType.INTEGER)


class FuncArrayCountItemInt(FuncArrayCountItem):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.INTEGER]),
    ]


class FuncArrayCountItemFloat(FuncArrayCountItem):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.FLOAT]),
    ]


class FuncArrayCountItemStr(FuncArrayCountItem):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_STR, DataType.STRING]),
    ]


class FuncArrayContains(FuncContains):
    arg_names = ["array", "value"]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.INTEGER]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.STRING]),
    ]


class FuncArrayNotContains(FuncNotContains):
    arg_names = ["array", "value"]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.INTEGER]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.STRING]),
    ]
    variants = [
        VW(
            D.DUMMY,
            lambda arr, val: n.not_(n.func.CONTAINS(arr, val)),
        ),
    ]


class FuncArrayContainsAll(ArrayFunction):
    name = "contains_all"
    arg_names = ["array_1", "array_2"]
    arg_cnt = 2
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION

    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.ARRAY_FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.ARRAY_STR]),
    ]


class FuncArrayContainsAny(ArrayFunction):
    name = "contains_any"
    arg_names = ["array_1", "array_2"]
    arg_cnt = 2
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION

    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.ARRAY_FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.ARRAY_STR]),
    ]


class FuncArrayContainsSubsequence(ArrayFunction):
    name = "contains_subsequence"
    arg_names = ["array_1", "array_2"]
    arg_cnt = 2
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION

    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.ARRAY_FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.ARRAY_STR]),
    ]


class FuncStartswithArrayBase(FuncStartswith):
    arg_names = ["array_1", "array_2"]


class FuncStartswithArrayConst(FuncStartswithArrayBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.CONST_ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.CONST_ARRAY_FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.CONST_ARRAY_STR]),
    ]


class FuncStartswithArrayNonConst(FuncStartswithArrayBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.ARRAY_FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.ARRAY_STR]),
    ]


class FuncArraySlice(ArrayFunction):
    name = "slice"
    arg_names = ["array", "offset", "length"]
    arg_cnt = 3
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.ARRAY_INT, DataType.ARRAY_FLOAT, DataType.ARRAY_STR},
                DataType.CONST_INTEGER,
                DataType.CONST_INTEGER,
            ]
        ),
    ]

    return_type = FromArgs(0)


class FuncReplaceArrayBase(ArrayFunction):
    name = "replace"
    arg_cnt = 3
    arg_names = ["array", "old", "new"]

    return_type = FromArgs(0)


class FuncReplaceArrayLiteralNull(FuncReplaceArrayBase):
    """
    This implementation is needed because the default one would return an array
    of Nullables making it impossible to use array aggregation functions
    """

    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.NULL, DataType.INTEGER]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.NULL, DataType.FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.NULL, DataType.STRING]),
    ]


class FuncReplaceArrayDefault(FuncReplaceArrayBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.INTEGER, DataType.INTEGER]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.STRING, DataType.STRING]),
    ]


class FuncArrayCast(ArrayFunction):
    arg_cnt = 1
    arg_names = ["array"]


class FuncIntArray(FuncArrayCast):
    name = "cast_arr_int"
    return_type = Fixed(DataType.ARRAY_INT)


class FuncIntArrayFromIntArray(FuncIntArray):
    variants = [
        V(D.DUMMY, lambda arr: arr),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT]),
    ]


class FuncIntArrayFromFloatArray(FuncIntArray):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
    ]


class FuncIntArrayFromStringArray(FuncIntArray):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_STR]),
    ]


class FuncFloatArray(FuncArrayCast):
    name = "cast_arr_float"
    return_type = Fixed(DataType.ARRAY_FLOAT)


class FuncFloatArrayFromIntArray(FuncFloatArray):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT]),
    ]


class FuncFloatArrayFromFloatArray(FuncFloatArray):
    variants = [
        V(D.DUMMY, lambda arr: arr),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
    ]


class FuncFloatArrayFromStringArray(FuncFloatArray):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_STR]),
    ]


class FuncStrArray(FuncArrayCast):
    name = "cast_arr_str"
    return_type = Fixed(DataType.ARRAY_STR)


class FuncStringArrayFromIntArray(FuncStrArray):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT]),
    ]


class FuncStringArrayFromFloatArray(FuncStrArray):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
    ]


class FuncStringArrayFromStringArray(FuncStrArray):
    variants = [
        V(D.DUMMY, lambda arr: arr),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_STR]),
    ]


class FuncArrayMinBase(ArrayFunction):
    name = "arr_min"
    arg_names = ["array"]
    arg_cnt = 1


class FuncArrayMinFloat(FuncArrayMinBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncArrayMinInt(FuncArrayMinBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT]),
    ]
    return_type = Fixed(DataType.INTEGER)


class FuncArrayMaxBase(ArrayFunction):
    name = "arr_max"
    arg_names = ["array"]
    arg_cnt = 1


class FuncArrayMaxFloat(FuncArrayMaxBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncArrayMaxInt(FuncArrayMaxBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT]),
    ]
    return_type = Fixed(DataType.INTEGER)


class FuncArraySumBase(ArrayFunction):
    name = "arr_sum"
    arg_names = ["array"]
    arg_cnt = 1


class FuncArraySumFloat(FuncArraySumBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncArraySumInt(FuncArraySumBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT]),
    ]
    return_type = Fixed(DataType.INTEGER)


class FuncArrayAvg(ArrayFunction):
    name = "arr_avg"
    arg_names = ["array"]
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([{DataType.ARRAY_INT, DataType.ARRAY_FLOAT}]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncArrayProduct(ArrayFunction):
    name = "arr_product"
    arg_names = ["array"]
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([{DataType.ARRAY_INT, DataType.ARRAY_FLOAT}]),
    ]
    return_type = Fixed(DataType.FLOAT)


class FuncArrayRemoveBase(ArrayFunction):
    name = "arr_remove"
    arg_names = ["array", "value"]
    arg_cnt = 2

    return_type = FromArgs(0)


class FuncArrayRemoveLiteralNull(FuncArrayRemoveBase):
    """
    This implementation is needed because the default one would return an array
    of Nullables making it impossible to use array aggregation functions
    """

    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.NULL]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.NULL]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.NULL]),
    ]


class FuncArrayRemoveDefault(FuncArrayRemoveBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.INTEGER]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.STRING]),
    ]


DEFINITIONS_ARRAY = [
    # arr_avg
    FuncArrayAvg,
    # arr_max
    FuncArrayMaxFloat,
    FuncArrayMaxInt,
    # arr_min
    FuncArrayMinFloat,
    FuncArrayMinInt,
    # arr_product
    FuncArrayProduct,
    # arr_remove
    FuncArrayRemoveLiteralNull,
    FuncArrayRemoveDefault,
    # arr_str
    FuncArrStr1,
    FuncArrStr2,
    FuncArrStr3,
    # arr_sum
    FuncArraySumFloat,
    FuncArraySumInt,
    # array
    FuncConstArrayFloat,
    FuncConstArrayInt,
    FuncConstArrayStr,
    FuncNonConstArrayInt,
    FuncNonConstArrayFloat,
    FuncNonConstArrayStr,
    # cast_arr_float
    FuncFloatArrayFromIntArray,
    FuncFloatArrayFromFloatArray,
    FuncFloatArrayFromStringArray,
    # cast_arr_int
    FuncIntArrayFromIntArray,
    FuncIntArrayFromFloatArray,
    FuncIntArrayFromStringArray,
    # cast_arr_str
    FuncStringArrayFromIntArray,
    FuncStringArrayFromFloatArray,
    FuncStringArrayFromStringArray,
    # contains
    FuncArrayContains,
    # notcontains
    FuncArrayNotContains,
    # contains_all
    FuncArrayContainsAll,
    # contains_any
    FuncArrayContainsAny,
    # contains_subsequence
    FuncArrayContainsSubsequence,
    # count_item
    FuncArrayCountItemInt,
    FuncArrayCountItemFloat,
    FuncArrayCountItemStr,
    # get_item
    FuncGetArrayItemFloat,
    FuncGetArrayItemInt,
    FuncGetArrayItemStr,
    # len
    FuncLenArray,
    # replace
    FuncReplaceArrayLiteralNull,
    FuncReplaceArrayDefault,
    # slice
    FuncArraySlice,
    # startswith
    FuncStartswithArrayConst,
    FuncStartswithArrayNonConst,
    # unnest
    FuncUnnestArrayFloat,
    FuncUnnestArrayInt,
    FuncUnnestArrayStr,
]
