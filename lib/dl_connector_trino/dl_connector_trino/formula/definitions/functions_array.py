from typing import cast

import sqlalchemy as sa
from sqlalchemy.sql.elements import (
    BinaryExpression,
    BindParameter,
    ClauseElement,
    ColumnClause,
)
from sqlalchemy.sql.expression import custom_op
from sqlalchemy.sql.functions import Function
import trino.sqlalchemy.datatype as tsa

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_array as base

from dl_connector_trino.formula.constants import TrinoDialect as D
from dl_connector_trino.formula.definitions.custom_constructors import (
    TrinoArray,
    TrinoLambda,
)


V = TranslationVariant.make

x_col, y_col = sa.column("x"), sa.column("y")

lambda_identical = TrinoLambda(x_col, x_col)
lambda_true = TrinoLambda(x_col, True)
lambda_is_null = TrinoLambda(x_col, x_col.is_(None))
lambda_is_not_null = TrinoLambda(x_col, x_col.isnot(None))
lambda_and = TrinoLambda(
    x_col,
    y_col,
    x_col & y_col,
)
lambda_is_not_distinct_from = TrinoLambda(
    x_col,
    y_col,
    BinaryExpression(x_col, y_col, custom_op("IS NOT DISTINCT FROM")),
)
lambda_format_float = TrinoLambda(
    x_col,
    sa.func.regexp_extract(
        sa.func.format("%.16f", x_col),
        "^(-?\\d+(\\.[1-9]+)?)(\\.?0*)$",
        1,
    ),
)
lambda_cast_double = TrinoLambda(
    x_col,
    sa.func.cast(x_col, tsa.DOUBLE()),
)
lambda_cast_bigint = TrinoLambda(
    x_col,
    sa.func.cast(x_col, sa.BIGINT()),
)
lambda_cast_varchar = TrinoLambda(
    x_col,
    sa.func.cast(x_col, sa.VARCHAR()),
)


def drop_null(array: ClauseElement) -> Function:
    return sa.func.filter(array, lambda_is_not_null)


def format_float(array_float: ClauseElement) -> Function:
    return sa.func.transform(
        array_float,
        lambda_format_float,
    )


def array_equals(x: ColumnClause, y: ColumnClause) -> Function:
    pairwise_non_distinct = sa.func.zip_with(
        x,
        y,
        lambda_is_not_distinct_from,
    )
    return sa.func.reduce(
        pairwise_non_distinct,
        True,
        lambda_and,
        lambda_identical,
    )


def array_startswith(x: ColumnClause, y: ColumnClause) -> Function:
    len_y = sa.func.cardinality(y)
    x_slice = sa.func.slice(x, 1, len_y)
    return sa.func.if_(
        sa.func.cardinality(x) < len_y,
        False,
        array_equals(
            x_slice,
            y,
        ),
    )


def array_intersect(*arrays: ColumnClause) -> Function:
    return sa.func.reduce(
        TrinoArray(*arrays[1:]),
        arrays[0],
        TrinoLambda(
            x_col,
            y_col,
            sa.func.array_intersect(x_col, y_col),
        ),
        lambda_identical,
    )


def count_item(array: ColumnClause, value: BindParameter) -> Function:
    return sa.func.if_(
        value.is_(None),
        sa.func.cardinality(sa.func.filter(array, lambda_is_null)),
        sa.func.cardinality(
            sa.func.filter(
                array,
                TrinoLambda(
                    x_col,
                    x_col == value,
                ),
            )
        ),
    )


def array_contains(array: ColumnClause, value: BindParameter) -> BinaryExpression:
    return cast(BinaryExpression, count_item(array, value) > 0)


def array_not_contains(array: ColumnClause, value: BindParameter) -> BinaryExpression:
    return cast(BinaryExpression, count_item(array, value) == 0)


def replace_array(array: ColumnClause, old_value: BindParameter, new_value: BindParameter) -> Function:
    return sa.func.if_(
        old_value.is_(None),
        sa.func.transform(
            array,
            TrinoLambda(
                x_col,
                sa.func.if_(
                    x_col.is_(None),
                    new_value,
                    x_col,
                ),
            ),
        ),
        sa.func.transform(
            array,
            TrinoLambda(
                x_col,
                sa.func.if_(
                    x_col == old_value,
                    new_value,
                    x_col,
                ),
            ),
        ),
    )


class FuncArrStr3Trino(base.FuncArrStr3):
    variants = [
        V(D.TRINO, lambda array, delimiter, null_str: sa.func.array_join(array, delimiter, null_str)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.CONST_STRING, DataType.CONST_STRING]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.CONST_STRING, DataType.CONST_STRING]),
    ]


class FuncFloatArrStr3Trino(base.FuncArrStr3):
    variants = [
        V(
            D.TRINO,
            lambda array, delimiter, null_str: sa.func.array_join(format_float(array), delimiter, null_str),
        ),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.CONST_STRING, DataType.CONST_STRING]),
    ]


class FuncArrStr2Trino(base.FuncArrStr2):
    variants = [
        V(D.TRINO, lambda array, delimiter: sa.func.array_join(drop_null(array), delimiter)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.CONST_STRING]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.CONST_STRING]),
    ]


class FuncFloatArrStr2Trino(base.FuncArrStr2):
    variants = [
        V(
            D.TRINO,
            lambda array, delimiter: sa.func.array_join(format_float(drop_null(array)), delimiter),
        ),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.CONST_STRING]),
    ]


class FuncArrStr1Trino(base.FuncArrStr1):
    variants = [
        V(D.TRINO, lambda array: sa.func.array_join(drop_null(array), ",")),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_STR]),
    ]


class FuncFloatArrStr1Trino(base.FuncArrStr1):
    variants = [
        V(
            D.TRINO,
            lambda array: sa.func.array_join(format_float(drop_null(array)), ","),
        ),
    ]
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
    ]


DEFINITIONS_ARRAY = [
    # arr_remove
    base.FuncArrayRemoveLiteralNull(
        variants=[
            V(
                D.TRINO,
                lambda array, _null: sa.func.filter(array, lambda_is_not_null),
            ),
        ]
    ),
    base.FuncArrayRemoveDefault(
        variants=[
            V(D.TRINO, sa.func.array_remove),
        ]
    ),
    # arr_str
    FuncArrStr1Trino(),
    FuncFloatArrStr1Trino(),
    FuncArrStr2Trino(),
    FuncFloatArrStr2Trino(),
    FuncArrStr3Trino(),
    FuncFloatArrStr3Trino(),
    # array
    base.FuncConstArrayFloat.for_dialect(D.TRINO),
    base.FuncConstArrayInt.for_dialect(D.TRINO),
    base.FuncConstArrayStr.for_dialect(D.TRINO),
    base.FuncNonConstArrayInt(
        variants=[
            V(D.TRINO, lambda *args: TrinoArray(*args)),
        ]
    ),
    # base.FuncNonConstArrayFloat.for_dialect(D.TRINO),
    base.FuncNonConstArrayStr(
        variants=[
            V(D.TRINO, lambda *args: TrinoArray(*args)),
        ]
    ),
    # cast_arr_float
    base.FuncFloatArrayFromIntArray(
        variants=[
            V(
                D.TRINO,
                lambda arr: sa.func.transform(arr, lambda_cast_double),
            ),
        ]
    ),
    base.FuncFloatArrayFromFloatArray.for_dialect(D.TRINO),
    base.FuncFloatArrayFromStringArray(
        variants=[
            V(
                D.TRINO,
                lambda arr: sa.func.transform(arr, lambda_cast_double),
            ),
        ]
    ),
    # cast_arr_int
    base.FuncIntArrayFromIntArray.for_dialect(D.TRINO),
    base.FuncIntArrayFromFloatArray(
        variants=[
            V(
                D.TRINO,
                lambda arr: sa.func.transform(arr, lambda_cast_bigint),
            ),
        ]
    ),
    base.FuncIntArrayFromStringArray(
        variants=[
            V(
                D.TRINO,
                lambda arr: sa.func.transform(arr, lambda_cast_bigint),
            ),
        ]
    ),
    # cast_arr_str
    base.FuncStringArrayFromIntArray(
        variants=[
            V(
                D.TRINO,
                lambda arr: sa.func.transform(arr, lambda_cast_varchar),
            ),
        ]
    ),
    base.FuncStringArrayFromFloatArray(
        variants=[
            V(D.TRINO, format_float),
        ]
    ),
    base.FuncStringArrayFromStringArray.for_dialect(D.TRINO),
    # contains
    base.FuncArrayContains(
        variants=[
            V(D.TRINO, array_contains),
        ]
    ),
    # notcontains
    base.FuncArrayNotContains(
        variants=[
            V(D.TRINO, array_not_contains),
        ]
    ),
    # contains_all
    base.FuncArrayContainsAll(
        variants=[
            V(
                D.TRINO,
                lambda array_1, array_2: sa.func.cardinality(sa.func.array_intersect(array_1, array_2))
                == sa.func.cardinality(sa.func.array_distinct(array_2)),
            ),
        ]
    ),
    # contains_any
    base.FuncArrayContainsAny(
        variants=[
            V(
                D.TRINO,
                lambda array_1, array_2: sa.func.cardinality(sa.func.array_intersect(array_1, array_2)) > 0,
            ),
        ]
    ),
    # count_item
    base.FuncArrayCountItemInt(
        variants=[
            V(D.TRINO, count_item),
        ]
    ),
    base.FuncArrayCountItemFloat(
        variants=[
            V(D.TRINO, count_item),
        ]
    ),
    base.FuncArrayCountItemStr(
        variants=[
            V(D.TRINO, count_item),
        ]
    ),
    # get_item
    base.FuncGetArrayItemFloat(
        variants=[
            V(D.TRINO, sa.func.element_at),
        ]
    ),
    base.FuncGetArrayItemInt(
        variants=[
            V(D.TRINO, sa.func.element_at),
        ]
    ),
    base.FuncGetArrayItemStr(
        variants=[
            V(D.TRINO, sa.func.element_at),
        ]
    ),
    # len
    base.FuncLenArray(
        variants=[
            V(D.TRINO, sa.func.cardinality),
        ]
    ),
    # replace
    # base.FuncReplaceArrayLiteralNull.for_dialect(D.TRINO),
    base.FuncReplaceArrayDefault(
        variants=[
            V(D.TRINO, replace_array),
        ]
    ),
    # slice
    base.FuncArraySlice(
        variants=[
            # V(D.TRINO, sa.func.slice),  # Due to bug in sqlalchemy, pure slice doesn't work
            V(
                D.TRINO,
                lambda array, start, offset: sa.func.filter(
                    sa.func.slice(array, start, offset),
                    lambda_true,
                ),
            ),
        ]
    ),
    # startswith
    base.FuncStartswithArrayConst(
        variants=[
            V(D.TRINO, array_startswith),
        ]
    ),
    base.FuncStartswithArrayNonConst(
        variants=[
            V(D.TRINO, array_startswith),
        ]
    ),
    # unnest
    # base.FuncUnnestArrayFloat.for_dialect(D.TRINO),
    # base.FuncUnnestArrayInt.for_dialect(D.TRINO),
    # base.FuncUnnestArrayStr.for_dialect(D.TRINO),
    # intersect
    base.FuncArrayIntersect(
        variants=[
            V(D.TRINO, array_intersect),
        ]
    ),
]
