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

count_item = lambda array, value: (
    sa.func.if_(
        value.is_(None),
        sa.func.cardinality(sa.func.filter(array, sa.text("x -> x IS NULL"))),
        sa.func.cardinality(
            sa.func.filter(array, sa.text(f"x -> x = {value.compile(compile_kwargs=dict(literal_binds=True))}"))
        ),
    )
)

contains = lambda array, value: (
    sa.func.if_(
        value.isnot(None),
        sa.func.contains(array, value),
        sa.func.cardinality(sa.func.filter(array, sa.text("x -> x IS NULL"))) > 0,
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
    # base.FuncArrayRemoveLiteralNull(
    #     variants=[
    #         V(D.TRINO, sa.func.array_remove),
    #     ]
    # ),
    # base.FuncArrayRemoveDefault(
    #     variants=[
    #         V(D.TRINO, sa.func.array_remove),
    #     ]
    # ),
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
    # base.FuncFloatArrayFromIntArray(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda arr: sa.func.array(sa.select(sa.cast(sa.func.unnest(arr), sa_postgresql.DOUBLE_PRECISION))),
    #         ),
    #     ]
    # ),
    # base.FuncFloatArrayFromFloatArray.for_dialect(D.TRINO),
    # base.FuncFloatArrayFromStringArray(
    #     variants=[
    #         V(D.TRINO, lambda arr: sa.func.array(sa.select(sa.cast(sa.func.unnest(arr), sa.FLOAT)))),
    #     ]
    # ),
    # cast_arr_int
    # base.FuncIntArrayFromIntArray.for_dialect(D.TRINO),
    # base.FuncIntArrayFromFloatArray(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda arr: sa.func.array(sa.select(sa.cast(sa.func.floor(sa.func.unnest(arr)), sa.BIGINT))),
    #         ),
    #     ]
    # ),
    # base.FuncIntArrayFromStringArray(
    #     variants=[
    #         V(D.TRINO, lambda arr: sa.func.array(sa.select(sa.cast(sa.func.unnest(arr), sa.BIGINT)))),
    #     ]
    # ),
    # cast_arr_str
    # base.FuncStringArrayFromIntArray(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda arr: sa.func.array(sa.select(sa.func.to_char(sa.func.unnest(arr), PG_INT_64_TO_CHAR_FMT))),
    #         ),
    #     ]
    # ),
    # base.FuncStringArrayFromFloatArray(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda arr: sa.func.array(sa.select(sa.func.to_char(sa.func.unnest(arr), "FM999999999990.0999999999"))),
    #         ),
    #     ]
    # ),
    # base.FuncStringArrayFromStringArray.for_dialect(D.TRINO),
    # Array contains(_all, _any) functions comment:
    # Straight-forward `array_1.contains(array_2)` doesn't work due to postgres specifics in NULL and arrays interaction
    # [NULL, 1, 2].contains([NULL, 1]) = False in postgres, that is why branching on null containment needed
    # In PostgreSQL 9.5+ use sa.func.array_position(array, None) != None instead of array_remove comparison
    # contains
    base.FuncArrayContains(
        variants=[
            V(D.TRINO, contains),
        ]
    ),
    # notcontains
    base.FuncArrayNotContains(
        variants=[
            V(D.TRINO, contains),
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
    # base.FuncReplaceArrayDefault(
    #     variants=[
    #         V(D.TRINO, sa.func.array_replace),
    #     ]
    # ),
    # slice
    base.FuncArraySlice(
        variants=[
            V(D.TRINO, sa.func.slice),
        ]
    ),
    # startswith
    # base.FuncStartswithArrayConst(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda x, y, _env: Grouping(sa.type_coerce(x, sa_postgresql.ARRAY(TypeEngine)))[1 : len(un_literal(y))]  # type: ignore  # 2024-01-24 # TODO: Invalid index type "slice" for "Grouping"; expected type "int"  [index]
    #             == y,
    #         ),
    #     ]
    # ),
    # base.FuncStartswithArrayNonConst(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda x, y, _env: Grouping(sa.type_coerce(x, sa_postgresql.ARRAY(TypeEngine)))[  # type: ignore  # 2024-01-24 # TODO: Invalid index type "slice" for "Grouping"; expected type "int"  [index]
    #                 1 : sa.func.array_length(y, 1)
    #             ]
    #             == y,
    #         ),
    #     ]
    # ),
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
    # base.FuncArrayIntersect(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda *arrays: sa.func.array(
    #                 sa.intersect(*[sa.select(sa.func.unnest(arr)) for arr in arrays]).scalar_subquery()
    #             ),
    #         )
    #     ]
    # ),
]
