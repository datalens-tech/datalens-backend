from typing import Callable

import sqlalchemy as sa
from sqlalchemy.sql.elements import (
    ClauseElement,
    ColumnElement,
    Grouping,
)
from sqlalchemy.sql.functions import Function
from sqlalchemy.sql.operators import ColumnOperators
from sqlalchemy.sql.type_api import TypeEngine
import ydb_sqlalchemy as ydb_sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_array as base
import dl_sqlalchemy_ydb.dialect as ydb_dialect

from dl_connector_ydb.formula.constants import YqlDialect as D


V = TranslationVariant.make

# Notes:
# - Use type_coerce to make sqlalchemy believe that value is an array and supports indexing


def _get_item(array: ColumnElement, index: ColumnElement) -> ColumnOperators:
    """
    Get array item and make sqlalchemy believe that value is indexable
    """

    return sa.type_coerce(array, sa.types.ARRAY(TypeEngine))[index]


def _get_tuple_item(tupl: ColumnElement, index: int) -> ColumnElement:
    """
    Get YQL tuple item using YqlDotAccess
    """

    return ydb_dialect.YqlDotAccess(
        Grouping(tupl),
        index,
    )


def _compare_with_nulls(
    a: ClauseElement,
    b: ClauseElement,
    if_true: ClauseElement,
    if_false: ClauseElement,
) -> Function:
    """
    Transformed into:
    ```yql
    IF(
        (a IS NULL) AND (b IS NULL) OR COALESCE(a == b, FALSE),
        if_true,
        if_false,
    )
    ```

    Which means one of the following condition:
    - Both a and b are NULL
    - a == b (if a or b is NULL, value should be FALSE)

    Note: Can not just write `(a == None) and (b == None) or sa.func.COALESCE(a == b, False)` because Boolean value of this clause is not defined
    """

    return sa.func.IF(
        # Both NULL
        a == None,
        sa.func.IF(
            b == None,
            if_true,
            if_false,
        ),
        # Equal
        sa.func.IF(
            sa.func.COALESCE(a == b, False),
            if_true,
            if_false,
        ),
    )


def _func_array_replace(array_value: ClauseElement, old_value: ClauseElement, new_value: ClauseElement) -> Function:
    return ydb_dialect.YqlClosure(
        lambda array, old, new: sa.func.ListMap(
            array,
            ydb_sa.types.Lambda(
                lambda item: _compare_with_nulls(
                    item,
                    old,
                    new,
                    item,
                )
            ),
        ),
        array_value,
        old_value,
        new_value,
    )


def _func_array_index_of(array_value: ClauseElement, value_value: ClauseElement) -> Function:
    return ydb_dialect.YqlClosure(
        lambda array, value: sa.func.ListFold(
            sa.func.ListEnumerate(array),
            sa.cast(0, ydb_sa.types.UInt64),
            ydb_sa.types.Lambda(
                lambda item, state: sa.func.IF(
                    state != 0,
                    state,
                    _compare_with_nulls(
                        _get_tuple_item(item, 1),
                        value,
                        _get_tuple_item(item, 0) + sa.cast(1, ydb_sa.types.UInt64),
                        state,
                    ),
                ),
            ),
        ),
        array_value,
        value_value,
    )


def _func_array_distinct(array: ClauseElement) -> Function:
    # Find element by value and return index
    # Note: If array is null, returns null
    # Note: If value not found, returns 0
    # Note: index starts with 1
    return sa.func.IF(
        array == None,
        None,
        sa.func.ListUniq(array),
    )


def _func_array_intersect(*arrays: ClauseElement) -> Function:
    return sa.func.DictKeys(
        sa.func.ListFold1(
            sa.func.AsList(*[sa.func.ToSet(array) for array in arrays]),
            ydb_sa.types.Lambda(
                lambda x: x,
            ),
            ydb_sa.types.Lambda(
                lambda item, state: sa.func.SetIntersection(item, state),
            ),
        ),
    )


def _make_func_get_array_item(item_type: type) -> Callable[[ClauseElement, ClauseElement], ClauseElement]:
    return lambda array_value, index_value: ydb_dialect.YqlClosure(
        lambda array, index: sa.cast(
            _get_item(array, index - 1),
            (item_type),
        ),
        array_value,
        index_value,
    )


def _array_count_item(array_value: ClauseElement, value_value: ClauseElement) -> ClauseElement:
    return ydb_dialect.YqlClosure(
        lambda array, value: sa.func.ListFold(
            array,
            0,
            ydb_sa.types.Lambda(
                lambda item, state: _compare_with_nulls(
                    item,
                    value,
                    state + 1,
                    state,
                ),
            ),
        ),
        array_value,
        value_value,
    )


def _func_array_contains(array_value: ClauseElement, item_value: ClauseElement) -> ClauseElement:
    return ydb_dialect.YqlClosure(
        lambda array, value: sa.func.ListAny(
            sa.func.ListMap(
                array,
                ydb_sa.types.Lambda(
                    lambda item: _compare_with_nulls(
                        item,
                        value,
                        sa.cast(True, sa.types.BOOLEAN),
                        sa.cast(False, sa.types.BOOLEAN),
                    )
                ),
            ),
        ),
        array_value,
        item_value,
    )


def _func_array_not_contains(array_value: ClauseElement, item_value: ClauseElement) -> ClauseElement:
    return ydb_dialect.YqlClosure(
        lambda array, value: sa.func.ListAll(
            sa.func.ListMap(
                array,
                ydb_sa.types.Lambda(
                    lambda item: _compare_with_nulls(
                        item,
                        value,
                        sa.cast(False, sa.types.BOOLEAN),
                        sa.cast(True, sa.types.BOOLEAN),
                    )
                ),
            ),
        ),
        array_value,
        item_value,
    )


def _func_array_contains_all(array_1: ClauseElement, array_2: ClauseElement) -> Function:
    return sa.func.DictLength(
        sa.func.SetIntersection(
            sa.func.ToSet(array_1),
            sa.func.ToSet(array_2),
        ),
    ) == sa.func.DictLength(sa.func.ToSet(array_2))


def _func_array_contains_any(array_1: ClauseElement, array_2: ClauseElement) -> Function:
    return (
        sa.func.DictLength(
            sa.func.SetIntersection(
                sa.func.ToSet(array_1),
                sa.func.ToSet(array_2),
            ),
        )
        != 0
    )


def _func_arr_str_1(array: ClauseElement) -> Function:
    return sa.func.ListConcat(
        sa.func.ListMap(
            sa.func.ListNotNull(
                array,
            ),
            ydb_sa.types.Lambda(lambda x: sa.cast(x, sa.types.TEXT)),
        ),
        ",",
    )


def _func_arr_str_2(array: ClauseElement, sep: ClauseElement) -> Function:
    return sa.func.ListConcat(
        sa.func.ListMap(
            sa.func.ListNotNull(
                array,
            ),
            ydb_sa.types.Lambda(lambda x: sa.cast(x, sa.types.TEXT)),
        ),
        sep,
    )


def _func_arr_str_3(
    array_value: ClauseElement, sep_value: ClauseElement, null_str_value: ClauseElement
) -> ClauseElement:
    return ydb_dialect.YqlClosure(
        lambda array, sep, null_str: sa.func.ListConcat(
            sa.func.ListMap(
                array,
                ydb_sa.types.Lambda(
                    lambda x: sa.func.IF(
                        x == None,
                        null_str,
                        sa.cast(x, sa.types.TEXT),
                    ),
                ),
            ),
            sep,
        ),
        array_value,
        sep_value,
        null_str_value,
    )


def _func_array_remove_default(array_value: ClauseElement, value_value: ClauseElement) -> Function:
    return ydb_dialect.YqlClosure(
        lambda array, value: sa.func.ListFilter(
            array,
            ydb_sa.types.Lambda(
                lambda x: sa.func.COALESCE(x != value, True),
            ),
        ),
        array_value,
        value_value,
    )


def _func_array_slice(array_value: ClauseElement, offset_value: ClauseElement, length_value: ClauseElement) -> Function:
    # What: Cut off (skip) left part (offset) and take center part (length)
    return ydb_dialect.YqlClosure(
        lambda array, offset, length: sa.func.ListMap(
            sa.func.ListTakeWhile(
                sa.func.ListSkipWhile(
                    sa.func.ListEnumerate(
                        array,
                    ),
                    ydb_sa.types.Lambda(
                        lambda pair: (_get_tuple_item(pair, 0) + 1) < offset,
                    ),
                ),
                ydb_sa.types.Lambda(
                    lambda pair: (_get_tuple_item(pair, 0) + 1) < (offset + length),
                ),
            ),
            ydb_sa.types.Lambda(
                lambda pair: _get_tuple_item(pair, 1),
            ),
        ),
        array_value,
        offset_value,
        length_value,
    )


def _compare_arrays(array_1_value: ClauseElement, array_2_value: ClauseElement) -> Function:
    return ydb_dialect.YqlClosure(
        lambda array_1, array_2: sa.func.IF(
            sa.func.ListLength(array_1) == sa.func.ListLength(array_2),
            sa.func.ListAll(
                sa.func.ListMap(
                    sa.func.ListZip(array_1, array_2),
                    ydb_sa.types.Lambda(
                        lambda pair: _compare_with_nulls(
                            _get_tuple_item(pair, 0),
                            _get_tuple_item(pair, 1),
                            sa.cast(True, sa.types.BOOLEAN),
                            sa.cast(False, sa.types.BOOLEAN),
                        )
                    ),
                )
            ),
            False,
        ),
        array_1_value,
        array_2_value,
    )


def _func_array_starts_with_array_const(array_1: ClauseElement, array_2: ClauseElement) -> Function:
    # What: slice array_1 using _func_array_slice and compare to array_2

    if not isinstance(array_2, ydb_dialect.YqlListLiteral):
        return _func_array_starts_with_array_non_const(
            array_1,
            array_2,
        )

    prefix_array_1 = _func_array_slice(
        array_1,
        sa.cast(1, ydb_sa.types.UInt64),
        sa.cast(len(array_2.clauses), ydb_sa.types.UInt64),
    )

    return _compare_arrays(
        prefix_array_1,
        array_2,
    )


def _func_array_starts_with_array_non_const(array_1: ClauseElement, array_2: ClauseElement) -> Function:
    # What: slice array_1 using _func_array_slice and compare to array_2

    prefix_array_1 = _func_array_slice(
        array_1,
        sa.cast(1, ydb_sa.types.UInt64),
        sa.func.ListLength(array_2),
    )

    return _compare_arrays(
        prefix_array_1,
        array_2,
    )


DEFINITIONS_ARRAY = [
    # arr_remove
    base.FuncArrayRemoveLiteralNull(
        # Remove all NULLs
        variants=[
            V(
                D.YQL,
                lambda array, null: sa.func.ListNotNull(array),
            ),
        ]
    ),
    base.FuncArrayRemoveDefault(
        # Remove all values equal to given
        variants=[
            V(
                D.YQL,
                _func_array_remove_default,
            ),
        ]
    ),
    # arr_str
    base.FuncArrStr1(
        # Concatenate all elements except NULL.
        #  Implicit cast every element to string.
        # Example result: `a,b,c`
        variants=[
            V(
                D.YQL,
                _func_arr_str_1,
            ),
        ]
    ),
    base.FuncArrStr2(
        # Concatenate all elements except NULL.
        #  Implicit cast every element to string.
        #  Use user-specified separator.
        # Example result: `a;b;c`
        variants=[
            V(
                D.YQL,
                _func_arr_str_2,
            ),
        ]
    ),
    base.FuncArrStr3(
        # Concatenate all elements except NULL.
        #  Implicit cast every element to string.
        #  Use user-specified separator.
        #  Use user-specified null placeholder.
        # Example result: `a,b,c`
        variants=[
            V(
                D.YQL,
                _func_arr_str_3,
            ),
        ]
    ),
    # array
    base.FuncConstArrayFloat(
        variants=[
            V(
                D.YQL,
                ydb_dialect.YqlListLiteral,
            ),
        ]
    ),
    base.FuncConstArrayInt(
        variants=[
            V(
                D.YQL,
                ydb_dialect.YqlListLiteral,
            ),
        ]
    ),
    base.FuncConstArrayStr(
        variants=[
            V(
                D.YQL,
                ydb_dialect.YqlListLiteral,
            ),
        ]
    ),
    base.FuncNonConstArrayInt(
        variants=[
            V(
                D.YQL,
                sa.func.AsList,
            ),
        ]
    ),
    base.FuncNonConstArrayFloat(
        variants=[
            V(
                D.YQL,
                sa.func.AsList,
            ),
        ]
    ),
    base.FuncNonConstArrayStr(
        variants=[
            V(
                D.YQL,
                sa.func.AsList,
            ),
        ]
    ),
    # cast_arr_float
    base.FuncFloatArrayFromIntArray(
        variants=[
            V(D.YQL, lambda array: sa.cast(array, ydb_dialect.YqlOptionalItemListType(sa.types.FLOAT))),
        ]
    ),
    base.FuncFloatArrayFromFloatArray.for_dialect(D.YQL),
    base.FuncFloatArrayFromStringArray(
        variants=[
            V(D.YQL, lambda array: sa.cast(array, ydb_dialect.YqlOptionalItemListType(sa.types.FLOAT))),
        ]
    ),
    # cast_arr_int
    base.FuncIntArrayFromIntArray.for_dialect(D.YQL),
    base.FuncIntArrayFromFloatArray(
        variants=[
            V(D.YQL, lambda array: sa.cast(array, ydb_dialect.YqlOptionalItemListType(ydb_sa.types.Int64))),
        ]
    ),
    base.FuncIntArrayFromStringArray(
        variants=[
            V(D.YQL, lambda array: sa.cast(array, ydb_dialect.YqlOptionalItemListType(ydb_sa.types.Int64))),
        ]
    ),
    # cast_arr_str
    base.FuncStringArrayFromIntArray(
        variants=[
            V(D.YQL, lambda array: sa.cast(array, ydb_dialect.YqlOptionalItemListType(sa.types.TEXT))),
        ]
    ),
    base.FuncStringArrayFromFloatArray(
        variants=[
            V(D.YQL, lambda array: sa.cast(array, ydb_dialect.YqlOptionalItemListType(sa.types.TEXT))),
        ]
    ),
    base.FuncStringArrayFromStringArray.for_dialect(D.YQL),
    # contains
    base.FuncArrayContains(
        # Note: Can't use YQL ListHas because in case of non-nullable items array,
        #  lookup of null in array is not supported
        variants=[
            V(
                D.YQL,
                _func_array_contains,
            ),
        ]
    ),
    # notcontains
    base.FuncArrayNotContains(
        # Note: Can't use YQL ListHas because in case of non-nullable items array,
        #  lookup of null in array is not supported
        variants=[
            V(
                D.YQL,
                _func_array_not_contains,
            ),
        ]
    ),
    # contains_all
    base.FuncArrayContainsAll(
        # What: array_1 contains all items from array_2.
        # Implementation:
        # - Convert both arrays to sets
        # - Intersect
        # - Check that size of resulting set is the same as size of set based on array_2
        variants=[
            V(
                D.YQL,
                _func_array_contains_all,
            ),
        ]
    ),
    # contains_any
    base.FuncArrayContainsAny(
        variants=[
            V(
                D.YQL,
                _func_array_contains_any,
            ),
        ]
    ),
    # count_item
    base.FuncArrayCountItemInt(
        # Count item in array.
        # Item expected to be non-NULL.
        variants=[
            V(
                D.YQL,
                _array_count_item,
            ),
        ]
    ),
    base.FuncArrayCountItemFloat(
        # Count item in array.
        # Item expected to be non-NULL.
        variants=[
            V(
                D.YQL,
                _array_count_item,
            ),
        ]
    ),
    base.FuncArrayCountItemStr(
        # Count item in array.
        # Item expected to be non-NULL.
        variants=[
            V(
                D.YQL,
                _array_count_item,
            ),
        ]
    ),
    # get_item
    base.FuncGetArrayItemFloat(
        variants=[
            V(
                D.YQL,
                _make_func_get_array_item(sa.types.FLOAT),
            ),
        ]
    ),
    base.FuncGetArrayItemInt(
        variants=[
            V(
                D.YQL,
                _make_func_get_array_item(ydb_sa.types.Int64),
            ),
        ]
    ),
    base.FuncGetArrayItemStr(
        variants=[
            V(
                D.YQL,
                _make_func_get_array_item(sa.types.TEXT),
            ),
        ]
    ),
    # len
    base.FuncLenArray(
        variants=[
            V(
                D.YQL,
                sa.func.ListLength,
            ),
        ]
    ),
    # replace
    base.FuncReplaceArrayLiteralNull(
        variants=[
            V(
                D.YQL,
                _func_array_replace,
            ),
        ]
    ),
    base.FuncReplaceArrayDefault(
        variants=[
            V(
                D.YQL,
                _func_array_replace,
            ),
        ]
    ),
    # slice
    base.FuncArraySlice(
        variants=[
            V(
                D.YQL,
                _func_array_slice,
            ),
        ]
    ),
    # startswith
    base.FuncStartswithArrayConst(
        variants=[
            V(
                D.YQL,
                _func_array_starts_with_array_const,
            ),
        ]
    ),
    base.FuncStartswithArrayNonConst(
        variants=[
            V(
                D.YQL,
                _func_array_starts_with_array_non_const,
            ),
        ]
    ),
    # TODO: unnest can be implemented using FLATTEN LIST BY
    # intersect
    base.FuncArrayIntersect(
        # Intersect all arrays and return result as an array
        variants=[
            V(
                D.YQL,
                _func_array_intersect,
            )
        ]
    ),
    # distinct
    base.FuncArrayDistinct(
        # Leave only unique items in array, order not preserved
        variants=[
            V(
                D.YQL,
                _func_array_distinct,
            ),
        ]
    ),
    # arr_index_of
    base.FuncArrayIndexOf(
        # Find element by value and return index
        variants=[
            V(
                D.YQL,
                _func_array_index_of,
            ),
        ]
    ),
]
