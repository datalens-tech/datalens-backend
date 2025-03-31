import sqlalchemy as sa
from sqlalchemy.sql.elements import (
    ClauseElement,
    TextClause,
)

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_array as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


def create_non_const_array(*args: list[ClauseElement]) -> TextClause:
    compiled_args = []
    for arg in args:
        if isinstance(arg, ClauseElement):
            compiled_args.append(str(arg.compile(compile_kwargs={"literal_binds": True})))
        elif arg is None:
            compiled_args.append("NULL")
        else:
            compiled_args.append(str(arg))

    array_elements = ",".join(compiled_args)
    return sa.text(f"ARRAY[{array_elements}]")


drop_null = lambda array: sa.func.filter(array, sa.text("x -> x IS NOT NULL"))
format_float = lambda array_float: sa.func.transform(
    array_float, sa.text("x -> regexp_extract(format('%.8f', x), '^(-?\d+(\.[1-9]+)?)(\.?0*)$', 1)")
)


def array_equals(x: ClauseElement, y: ClauseElement) -> ClauseElement:
    pairwise_non_distinct = sa.func.zip_with(
        x,
        y,
        sa.text("(x, y) -> x IS NOT DISTINCT FROM y"),
    )
    return sa.func.reduce(
        pairwise_non_distinct,
        True,
        sa.text("(x, y) -> x AND y"),
        sa.text("x -> x"),
    )


def array_startswith(x: ClauseElement, y: ClauseElement) -> ClauseElement:
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


def array_intersect(*arrays: ClauseElement) -> ClauseElement:
    return sa.func.reduce(
        sa.text(
            f"ARRAY[{','.join([str(arr.compile(compile_kwargs=dict(literal_binds=True))) for arr in arrays[1:]])}]"
        ),
        arrays[0],
        sa.text("(x, y) -> array_intersect(x, y)"),
        sa.text("x -> x"),
    )


count_item = lambda array, value: (
    sa.func.if_(
        value.is_(None),
        sa.func.cardinality(sa.func.filter(array, sa.text("x -> x IS NULL"))),
        sa.func.cardinality(
            sa.func.filter(array, sa.text(f"x -> x = {value.compile(compile_kwargs=dict(literal_binds=True))}"))
        ),
    )
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
            V(D.TRINO, lambda array, _null: sa.func.filter(array, sa.text("x -> x IS NOT NULL"))),
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
            V(D.TRINO, create_non_const_array),
        ]
    ),
    # base.FuncNonConstArrayFloat(
    #     variants=[
    #         V(D.TRINO, lambda *args: sa_postgresql.array(args)),
    #     ]
    # ),
    base.FuncNonConstArrayStr(
        variants=[
            V(D.TRINO, create_non_const_array),
        ]
    ),
    # cast_arr_float
    base.FuncFloatArrayFromIntArray(
        variants=[
            V(
                D.TRINO,
                lambda arr: sa.func.transform(arr, sa.text("x -> cast(x as double)")),
            ),
        ]
    ),
    base.FuncFloatArrayFromFloatArray.for_dialect(D.TRINO),
    base.FuncFloatArrayFromStringArray(
        variants=[
            V(
                D.TRINO,
                lambda arr: sa.func.transform(arr, sa.text("x -> cast(x as double)")),
            ),
        ]
    ),
    # cast_arr_int
    base.FuncIntArrayFromIntArray.for_dialect(D.TRINO),
    base.FuncIntArrayFromFloatArray(
        variants=[
            V(
                D.TRINO,
                lambda arr: sa.func.transform(arr, sa.text("x -> cast(x as bigint)")),
            ),
        ]
    ),
    base.FuncIntArrayFromStringArray(
        variants=[
            V(
                D.TRINO,
                lambda arr: sa.func.transform(arr, sa.text("x -> cast(x as bigint)")),
            ),
        ]
    ),
    # cast_arr_str
    base.FuncStringArrayFromIntArray(
        variants=[
            V(
                D.TRINO,
                lambda arr: sa.func.transform(arr, sa.text("x -> cast(x as varchar)")),
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
            V(D.TRINO, lambda array, value: count_item(array, value) > 0),
        ]
    ),
    # notcontains
    base.FuncArrayNotContains(
        variants=[
            V(D.TRINO, lambda array, value: count_item(array, value) == 0),
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
    # base.FuncReplaceArrayLiteralNull(
    #     variants=[
    #         V(D.TRINO, sa.func.array_replace),
    #     ]
    # ),
    base.FuncReplaceArrayDefault(
        variants=[
            V(
                D.TRINO,
                lambda array, old_value, new_value: sa.func.if_(
                    old_value.is_(None),
                    sa.func.transform(
                        array,
                        sa.text(f"x -> if(x IS NULL, {new_value.compile(compile_kwargs=dict(literal_binds=True))}, x)"),
                    ),
                    sa.func.transform(
                        array,
                        sa.text(
                            f"x -> if(x = {old_value.compile(compile_kwargs=dict(literal_binds=True))}, {new_value.compile(compile_kwargs=dict(literal_binds=True))}, x)"
                        ),
                    ),
                ),
            ),
        ]
    ),
    # slice
    base.FuncArraySlice(
        variants=[
            # V(D.TRINO, sa.func.slice),  # Due to bug in sqlalchemy, pure slice doesn't work
            V(
                D.TRINO,
                lambda array, start, offset: sa.func.filter(sa.func.slice(array, start, offset), sa.text("x -> true")),
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
    # base.FuncUnnestArrayFloat(
    #     variants=[
    #         V(D.TRINO, lambda arr: sa.func.unnest(arr)),
    #     ]
    # ),
    # base.FuncUnnestArrayInt(
    #     variants=[
    #         V(D.TRINO, lambda arr: sa.func.unnest(arr)),
    #     ]
    # ),
    # base.FuncUnnestArrayStr(
    #     variants=[
    #         V(D.TRINO, lambda arr: sa.func.unnest(arr)),
    #     ]
    # ),
    # intersect
    base.FuncArrayIntersect(
        variants=[
            V(D.TRINO, array_intersect),
        ]
    ),
]
