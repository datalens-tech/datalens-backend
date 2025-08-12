from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as sa_postgresql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.elements import (
    Grouping,
    Null,
)
from sqlalchemy.sql.type_api import TypeEngine

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_array as base
from dl_formula.definitions.literals import (
    is_literal,
    un_literal,
)
from dl_formula.shortcuts import n

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D
from dl_connector_postgresql.formula.definitions.common import PG_INT_64_TO_CHAR_FMT


V = TranslationVariant.make


def _array_index_of_legacy(array: ClauseElement, value: ClauseElement) -> ClauseElement:
    """Implementation for PostgreSQL < 9.5 without array_position function"""
    return sa.func.coalesce(
        sa.select(sa.func.generate_subscripts(array, 1))
        .where(sa.type_coerce(array, sa_postgresql.ARRAY(TypeEngine))[sa.func.generate_subscripts(array, 1)] == value)
        .order_by(sa.func.generate_subscripts(array, 1))
        .limit(1)
        .scalar_subquery(),
        0,
    )


def _array_index_of_modern(array: ClauseElement, value: ClauseElement) -> ClauseElement:
    """Implementation for PostgreSQL >= 9.5 with array_position function"""
    return sa.func.coalesce(sa.func.array_position(array, value), 0)


class ArrayIndexOf(ClauseElement):
    """Version-aware array_index_of implementation"""

    def __init__(self, array: ClauseElement, value: ClauseElement) -> None:
        self.array = array
        self.value = value
        self.type = sa.Integer()


@compiles(ArrayIndexOf)
def visit_array_index_of_default(element: ArrayIndexOf, compiler: SQLCompiler, **kw: Any) -> str:
    """Default compilation - use legacy for safety"""
    return compiler.process(_array_index_of_legacy(element.array, element.value), **kw)


@compiles(ArrayIndexOf, "postgresql")
def visit_array_index_of_postgresql(element: ArrayIndexOf, compiler: SQLCompiler, **kw: Any) -> str:
    """PostgreSQL-specific compilation with version detection"""
    dialect = compiler.dialect

    if hasattr(dialect, "server_version_info") and dialect.server_version_info >= (9, 5):
        return compiler.process(_array_index_of_modern(element.array, element.value), **kw)
    else:
        return compiler.process(_array_index_of_legacy(element.array, element.value), **kw)


def _array_index_of(array: ClauseElement, value: ClauseElement) -> ClauseElement:
    """Choose implementation based on PostgreSQL version"""
    return ArrayIndexOf(array, value)


def _array_contains(array: ClauseElement, value: ClauseElement) -> ClauseElement:
    if isinstance(value, Null):
        return array != sa.func.array_remove(array, None)
    elif is_literal(value):
        return value == sa.func.ANY(array)
    else:
        return n.func.IF(
            n.func.ISNULL(value.self_group()), array != sa.func.array_remove(array, None), value == sa.func.ANY(array)
        )


def _array_notcontains(array: ClauseElement, value: ClauseElement) -> ClauseElement:
    if isinstance(value, Null):
        return array == sa.func.array_remove(array, None)
    elif is_literal(value):
        return value != sa.func.ALL(sa.func.array_remove(array, None))
    else:
        return n.func.IF(
            n.func.ISNULL(value.self_group()),
            array == sa.func.array_remove(array, None),
            value != sa.func.ALL(sa.func.array_remove(array, None)),
        )


DEFINITIONS_ARRAY = [
    # arr_remove
    base.FuncArrayRemoveLiteralNull(
        variants=[
            V(D.POSTGRESQL, sa.func.array_remove),
        ]
    ),
    base.FuncArrayRemoveDefault(
        variants=[
            V(D.POSTGRESQL, sa.func.array_remove),
        ]
    ),
    # arr_str
    base.FuncArrStr1(
        variants=[
            V(D.POSTGRESQL, lambda array: sa.func.array_to_string(array, ",")),
        ]
    ),
    base.FuncArrStr2(
        variants=[
            V(D.POSTGRESQL, lambda array, sep: sa.func.array_to_string(array, sep)),
        ]
    ),
    base.FuncArrStr3(
        variants=[
            V(D.POSTGRESQL, lambda array, sep, null_str: sa.func.array_to_string(array, sep, null_str)),
        ]
    ),
    # array
    base.FuncConstArrayFloat.for_dialect(D.POSTGRESQL),
    base.FuncConstArrayInt.for_dialect(D.POSTGRESQL),
    base.FuncConstArrayStr.for_dialect(D.POSTGRESQL),
    base.FuncNonConstArrayInt(
        variants=[
            V(D.POSTGRESQL, lambda *args: sa_postgresql.array(args)),
        ]
    ),
    base.FuncNonConstArrayFloat(
        variants=[
            V(D.POSTGRESQL, lambda *args: sa_postgresql.array(args)),
        ]
    ),
    base.FuncNonConstArrayStr(
        variants=[
            V(D.POSTGRESQL, lambda *args: sa_postgresql.array(args)),
        ]
    ),
    # cast_arr_float
    base.FuncFloatArrayFromIntArray(
        variants=[
            V(
                D.POSTGRESQL,
                lambda arr: sa.func.array(sa.select(sa.cast(sa.func.unnest(arr), sa_postgresql.DOUBLE_PRECISION))),
            ),
        ]
    ),
    base.FuncFloatArrayFromFloatArray.for_dialect(D.POSTGRESQL),
    base.FuncFloatArrayFromStringArray(
        variants=[
            V(D.POSTGRESQL, lambda arr: sa.func.array(sa.select(sa.cast(sa.func.unnest(arr), sa.FLOAT)))),
        ]
    ),
    # cast_arr_int
    base.FuncIntArrayFromIntArray.for_dialect(D.POSTGRESQL),
    base.FuncIntArrayFromFloatArray(
        variants=[
            V(
                D.POSTGRESQL,
                lambda arr: sa.func.array(sa.select(sa.cast(sa.func.floor(sa.func.unnest(arr)), sa.BIGINT))),
            ),
        ]
    ),
    base.FuncIntArrayFromStringArray(
        variants=[
            V(D.POSTGRESQL, lambda arr: sa.func.array(sa.select(sa.cast(sa.func.unnest(arr), sa.BIGINT)))),
        ]
    ),
    # cast_arr_str
    base.FuncStringArrayFromIntArray(
        variants=[
            V(
                D.POSTGRESQL,
                lambda arr: sa.func.array(sa.select(sa.func.to_char(sa.func.unnest(arr), PG_INT_64_TO_CHAR_FMT))),
            ),
        ]
    ),
    base.FuncStringArrayFromFloatArray(
        variants=[
            V(
                D.POSTGRESQL,
                lambda arr: sa.func.array(sa.select(sa.func.to_char(sa.func.unnest(arr), "FM999999999990.0999999999"))),
            ),
        ]
    ),
    base.FuncStringArrayFromStringArray.for_dialect(D.POSTGRESQL),
    # Array contains(_all, _any) functions comment:
    # Straight-forward `array_1.contains(array_2)` doesn't work due to postgres specifics in NULL and arrays interaction
    # [NULL, 1, 2].contains([NULL, 1]) = False in postgres, that is why branching on null containment needed
    # In PostgreSQL 9.5+ use sa.func.array_position(array, None) != None instead of array_remove comparison
    # contains
    base.FuncArrayContains(
        variants=[
            V(D.POSTGRESQL, _array_contains),
        ]
    ),
    # notcontains
    base.FuncArrayNotContains(
        variants=[
            V(D.POSTGRESQL, _array_notcontains),
        ]
    ),
    # contains_all
    base.FuncArrayContainsAll(
        variants=[
            V(
                D.POSTGRESQL,
                lambda array_1, array_2: n.func.IF(
                    array_2 != sa.func.array_remove(array_2, None),  # array_2 contains null
                    n.func.IF(
                        array_1 != sa.func.array_remove(array_1, None),  # array_1 contains null
                        sa.type_coerce(array_1, sa_postgresql.ARRAY(TypeEngine)).contains(
                            sa.type_coerce(sa.func.array_remove(array_2, None), sa_postgresql.ARRAY(TypeEngine))
                        ),
                        False,
                    ),
                    sa.type_coerce(array_1, sa_postgresql.ARRAY(TypeEngine)).contains(
                        sa.type_coerce(array_2, sa_postgresql.ARRAY(TypeEngine))
                    ),
                ),
            ),
        ]
    ),
    # contains_any
    base.FuncArrayContainsAny(
        variants=[
            V(
                D.POSTGRESQL,
                lambda array_1, array_2: n.func.IF(
                    array_2 != sa.func.array_remove(array_2, None),  # array_2 contains null
                    n.func.IF(
                        array_1 != sa.func.array_remove(array_1, None),  # array_1 contains null
                        True,
                        sa.type_coerce(array_1, sa_postgresql.ARRAY(TypeEngine)).overlap(
                            sa.type_coerce(sa.func.array_remove(array_2, None), sa_postgresql.ARRAY(TypeEngine))
                        ),
                    ),
                    sa.type_coerce(array_1, sa_postgresql.ARRAY(TypeEngine)).overlap(
                        sa.type_coerce(array_2, sa_postgresql.ARRAY(TypeEngine))
                    ),
                ),
            ),
        ]
    ),
    # count_item
    base.FuncArrayCountItemInt(
        variants=[
            V(
                D.POSTGRESQL,
                lambda array, value: (
                    # count_item for null array should be null
                    n.func.IF(
                        array == None,
                        None,
                        sa.func.array_length(array, 1)
                        - sa.func.coalesce(sa.func.array_length(sa.func.array_remove(array, value), 1), 0),
                    )
                ),
            ),
        ]
    ),
    base.FuncArrayCountItemFloat(
        variants=[
            V(
                D.POSTGRESQL,
                lambda array, value: (
                    # count_item for null array should be null
                    n.func.IF(
                        array == None,
                        None,
                        sa.func.array_length(array, 1)
                        - sa.func.coalesce(
                            sa.func.array_length(
                                sa.func.array_remove(
                                    sa.cast(array, sa.ARRAY(sa_postgresql.DOUBLE_PRECISION)),
                                    sa.cast(value, sa_postgresql.DOUBLE_PRECISION),
                                ),
                                1,
                            ),
                            0,
                        ),
                    )
                ),
            ),
        ]
    ),
    base.FuncArrayCountItemStr(
        variants=[
            V(
                D.POSTGRESQL,
                lambda array, value: (
                    # count_item for null array should be null
                    n.func.IF(
                        array == None,
                        None,
                        sa.func.array_length(array, 1)
                        - sa.func.coalesce(sa.func.array_length(sa.func.array_remove(array, value), 1), 0),
                    )
                ),
            ),
        ]
    ),
    # get_item
    base.FuncGetArrayItemFloat(
        variants=[
            V(
                D.POSTGRESQL,
                # using Grouping as workaround to wrap expression before getitem with ()
                lambda array, index: Grouping(sa.type_coerce(array, sa_postgresql.ARRAY(TypeEngine)))[index],  # type: ignore  # 2024-01-24 # TODO: Argument 2 has incompatible type "Callable[[Any, Any], ColumnOperators[Any]]"; expected "Callable[..., ClauseElement | FormulaItem]"  [arg-type]
            ),
        ]
    ),
    base.FuncGetArrayItemInt(
        variants=[
            V(
                D.POSTGRESQL,
                # using Grouping as workaround to wrap expression before getitem with ()
                lambda array, index: Grouping(sa.type_coerce(array, sa_postgresql.ARRAY(TypeEngine)))[index],  # type: ignore  # 2024-01-24 # TODO: Argument 2 has incompatible type "Callable[[Any, Any], ColumnOperators[Any]]"; expected "Callable[..., ClauseElement | FormulaItem]"  [arg-type]
            ),
        ]
    ),
    base.FuncGetArrayItemStr(
        variants=[
            V(
                D.POSTGRESQL,
                # using Grouping as workaround to wrap expression before getitem with ()
                lambda array, index: Grouping(sa.type_coerce(array, sa_postgresql.ARRAY(TypeEngine)))[index],  # type: ignore  # 2024-01-24 # TODO: Argument 2 has incompatible type "Callable[[Any, Any], ColumnOperators[Any]]"; expected "Callable[..., ClauseElement | FormulaItem]"  [arg-type]
            ),
        ]
    ),
    # len
    base.FuncLenArray(
        variants=[
            # len for null array should be null
            V(
                D.POSTGRESQL,
                lambda val: n.func.IF(
                    val == None,
                    sa.cast(None, sa_postgresql.INTEGER),
                    sa.func.coalesce(sa.func.ARRAY_LENGTH(val, 1), 0),
                ),
            ),
        ]
    ),
    # replace
    base.FuncReplaceArrayLiteralNull(
        variants=[
            V(D.POSTGRESQL, sa.func.array_replace),
        ]
    ),
    base.FuncReplaceArrayDefault(
        variants=[
            V(D.POSTGRESQL, sa.func.array_replace),
        ]
    ),
    # slice
    base.FuncArraySlice(
        variants=[
            V(
                D.POSTGRESQL,
                lambda array, offset, length: Grouping(sa.type_coerce(array, sa_postgresql.ARRAY(TypeEngine)))[  # type: ignore  # 2024-01-24 # TODO: Argument 2 has incompatible type "Callable[[Any, Any, Any], ColumnOperators[Any]]"; expected "Callable[..., ClauseElement | FormulaItem]"  [arg-type]
                    offset : offset + length - 1
                ],
            ),
        ]
    ),
    # startswith
    base.FuncStartswithArrayConst(
        variants=[
            V(
                D.POSTGRESQL,
                lambda x, y, _env: Grouping(sa.type_coerce(x, sa_postgresql.ARRAY(TypeEngine)))[1 : len(un_literal(y))]  # type: ignore  # 2024-01-24 # TODO: Invalid index type "slice" for "Grouping"; expected type "int"  [index]
                == y,
            ),
        ]
    ),
    base.FuncStartswithArrayNonConst(
        variants=[
            V(
                D.POSTGRESQL,
                lambda x, y, _env: Grouping(sa.type_coerce(x, sa_postgresql.ARRAY(TypeEngine)))[  # type: ignore  # 2024-01-24 # TODO: Invalid index type "slice" for "Grouping"; expected type "int"  [index]
                    1 : sa.func.array_length(y, 1)
                ]
                == y,
            ),
        ]
    ),
    # unnest
    base.FuncUnnestArrayFloat(
        variants=[
            V(D.POSTGRESQL, lambda arr: sa.func.unnest(arr)),
        ]
    ),
    base.FuncUnnestArrayInt(
        variants=[
            V(D.POSTGRESQL, lambda arr: sa.func.unnest(arr)),
        ]
    ),
    base.FuncUnnestArrayStr(
        variants=[
            V(D.POSTGRESQL, lambda arr: sa.func.unnest(arr)),
        ]
    ),
    # intersect
    base.FuncArrayIntersect(
        variants=[
            V(
                D.POSTGRESQL,
                lambda *arrays: sa.func.array(
                    sa.intersect(*[sa.select(sa.func.unnest(arr)) for arr in arrays]).scalar_subquery()
                ),
            )
        ]
    ),
    # distinct
    base.FuncArrayDistinctStr(
        variants=[
            V(
                D.POSTGRESQL,
                lambda array: n.func.IF(
                    array == None,
                    None,
                    sa.func.array(sa.select(sa.func.unnest(array)).distinct().scalar_subquery()),
                ),
            ),
        ]
    ),
    base.FuncArrayDistinctInt(
        variants=[
            V(
                D.POSTGRESQL,
                lambda array: n.func.IF(
                    array == None,
                    None,
                    sa.func.array(sa.select(sa.func.unnest(array)).distinct().scalar_subquery()),
                ),
            ),
        ]
    ),
    base.FuncArrayDistinctFloat(
        variants=[
            V(
                D.POSTGRESQL,
                lambda array: n.func.IF(
                    array == None,
                    None,
                    sa.func.array(sa.select(sa.func.unnest(array)).distinct().scalar_subquery()),
                ),
            ),
        ]
    ),
    # arr_index_of
    base.FuncArrayIndexOfStr(
        variants=[
            V(
                D.POSTGRESQL,
                _array_index_of,
            ),
        ]
    ),
    base.FuncArrayIndexOfInt(
        variants=[
            V(
                D.POSTGRESQL,
                _array_index_of,
            ),
        ]
    ),
    base.FuncArrayIndexOfFloat(
        variants=[
            V(
                D.POSTGRESQL,
                _array_index_of,
            ),
        ]
    ),
]
