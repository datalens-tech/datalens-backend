import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_array as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


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
    # base.FuncArrStr1(
    #     variants=[
    #         V(D.TRINO, lambda array: sa.func.array_to_string(array, ",")),
    #     ]
    # ),
    # base.FuncArrStr2(
    #     variants=[
    #         V(D.TRINO, lambda array, sep: sa.func.array_to_string(array, sep)),
    #     ]
    # ),
    # base.FuncArrStr3(
    #     variants=[
    #         V(D.TRINO, lambda array, sep, null_str: sa.func.array_to_string(array, sep, null_str)),
    #     ]
    # ),
    # array
    base.FuncConstArrayFloat.for_dialect(D.TRINO),
    base.FuncConstArrayInt.for_dialect(D.TRINO),
    base.FuncConstArrayStr.for_dialect(D.TRINO),
    # base.FuncNonConstArrayInt(
    #     variants=[
    #         V(D.TRINO, lambda *args: sa_postgresql.array(args)),
    #     ]
    # ),
    # base.FuncNonConstArrayFloat(
    #     variants=[
    #         V(D.TRINO, lambda *args: sa_postgresql.array(args)),
    #     ]
    # ),
    # base.FuncNonConstArrayStr(
    #     variants=[
    #         V(D.TRINO, lambda *args: sa_postgresql.array(args)),
    #     ]
    # ),
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
    # base.FuncArrayContains(
    #     variants=[
    #         V(D.TRINO, _array_contains),
    #     ]
    # ),
    # notcontains
    # base.FuncArrayNotContains(
    #     variants=[
    #         V(D.TRINO, _array_notcontains),
    #     ]
    # ),
    # contains_all
    # base.FuncArrayContainsAll(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda array_1, array_2: n.func.IF(
    #                 array_2 != sa.func.array_remove(array_2, None),  # array_2 contains null
    #                 n.func.IF(
    #                     array_1 != sa.func.array_remove(array_1, None),  # array_1 contains null
    #                     sa.type_coerce(array_1, sa_postgresql.ARRAY(TypeEngine)).contains(
    #                         sa.type_coerce(sa.func.array_remove(array_2, None), sa_postgresql.ARRAY(TypeEngine))
    #                     ),
    #                     False,
    #                 ),
    #                 sa.type_coerce(array_1, sa_postgresql.ARRAY(TypeEngine)).contains(
    #                     sa.type_coerce(array_2, sa_postgresql.ARRAY(TypeEngine))
    #                 ),
    #             ),
    #         ),
    #     ]
    # ),
    # contains_any
    # base.FuncArrayContainsAny(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda array_1, array_2: n.func.IF(
    #                 array_2 != sa.func.array_remove(array_2, None),  # array_2 contains null
    #                 n.func.IF(
    #                     array_1 != sa.func.array_remove(array_1, None),  # array_1 contains null
    #                     True,
    #                     sa.type_coerce(array_1, sa_postgresql.ARRAY(TypeEngine)).overlap(
    #                         sa.type_coerce(sa.func.array_remove(array_2, None), sa_postgresql.ARRAY(TypeEngine))
    #                     ),
    #                 ),
    #                 sa.type_coerce(array_1, sa_postgresql.ARRAY(TypeEngine)).overlap(
    #                     sa.type_coerce(array_2, sa_postgresql.ARRAY(TypeEngine))
    #                 ),
    #             ),
    #         ),
    #     ]
    # ),
    # count_item
    # base.FuncArrayCountItemInt(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda array, value: (
    #                 sa.func.array_length(array, 1) - sa.func.array_length(sa.func.array_remove(array, value), 1)
    #             ),
    #         ),
    #     ]
    # ),
    # base.FuncArrayCountItemFloat(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda array, value: (
    #                 sa.func.array_length(array, 1)
    #                 - sa.func.array_length(
    #                     sa.func.array_remove(array, sa.cast(value, sa_postgresql.DOUBLE_PRECISION)), 1
    #                 )
    #             ),
    #         ),
    #     ]
    # ),
    # base.FuncArrayCountItemStr(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda array, value: (
    #                 sa.func.array_length(array, 1) - sa.func.array_length(sa.func.array_remove(array, value), 1)
    #             ),
    #         ),
    #     ]
    # ),
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
    # base.FuncLenArray(
    #     variants=[
    #         V(D.TRINO, lambda val: sa.func.ARRAY_LENGTH(val, 1)),
    #     ]
    # ),
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
    # base.FuncArraySlice(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda array, offset, length: Grouping(sa.type_coerce(array, sa_postgresql.ARRAY(TypeEngine)))[  # type: ignore  # 2024-01-24 # TODO: Argument 2 has incompatible type "Callable[[Any, Any, Any], ColumnOperators[Any]]"; expected "Callable[..., ClauseElement | FormulaItem]"  [arg-type]
    #                 offset : offset + length - 1
    #             ],
    #         ),
    #     ]
    # ),
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
