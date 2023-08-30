import sqlalchemy as sa
from clickhouse_sqlalchemy import types as ch_types
from clickhouse_sqlalchemy.ext.clauses import Lambda

import bi_formula.definitions.functions_array as base
from bi_formula.definitions.base import TranslationVariant
from bi_formula.definitions.literals import un_literal

from bi_connector_clickhouse.formula.constants import ClickHouseDialect as D


V = TranslationVariant.make


DEFINITIONS_ARRAY = [
    # arr_avg
    base.FuncArrayAvg(variants=[
        V(D.and_above(D.CLICKHOUSE_21_8), sa.func.arrayAvg),
    ]),

    # arr_max
    base.FuncArrayMaxFloat(variants=[
        V(D.and_above(D.CLICKHOUSE_21_8), sa.func.arrayMax),
    ]),
    base.FuncArrayMaxInt(variants=[
        V(D.and_above(D.CLICKHOUSE_21_8), sa.func.arrayMax),
    ]),

    # arr_min
    base.FuncArrayMinFloat(variants=[
        V(D.and_above(D.CLICKHOUSE_21_8), sa.func.arrayMin),
    ]),
    base.FuncArrayMinInt(variants=[
        V(D.and_above(D.CLICKHOUSE_21_8), sa.func.arrayMin),
    ]),

    # arr_product
    base.FuncArrayProduct(variants=[
        V(D.and_above(D.CLICKHOUSE_21_8), sa.func.arrayProduct),
    ]),

    # arr_remove
    base.FuncArrayRemoveLiteralNull(variants=[
        V(D.CLICKHOUSE, lambda array, value: sa.func.arrayMap(
            Lambda(lambda x: sa.func.assumeNotNull(x)),
            sa.func.arrayFilter(Lambda(lambda x: sa.func.isNotNull(x)), array)
        )),
    ]),
    base.FuncArrayRemoveDefault(variants=[
        V(D.CLICKHOUSE, lambda array, value: sa.func.arrayFilter(
            Lambda(lambda x: sa.func.ifNull(x != value, 1)), array
        )),
    ]),

    # arr_str
    base.FuncArrStr1(variants=[
        V(
            D.CLICKHOUSE,
            lambda array: sa.func.arrayStringConcat(
                sa.func.arrayMap(
                    Lambda(lambda x: sa.cast(x, ch_types.String)),
                    sa.func.arrayFilter(Lambda(lambda x: sa.func.isNotNull(x)), array)
                ),
                ','
            )
        ),
    ]),
    base.FuncArrStr2(variants=[
        V(
            D.CLICKHOUSE,
            lambda array, sep: sa.func.arrayStringConcat(
                sa.func.arrayMap(
                    Lambda(lambda x: sa.cast(x, ch_types.String)),
                    sa.func.arrayFilter(Lambda(lambda x: sa.func.isNotNull(x)), array)
                ),
                sep
            )
        ),
    ]),
    base.FuncArrStr3(variants=[
        V(
            D.CLICKHOUSE,
            lambda array, sep, null_str: sa.func.arrayStringConcat(
                sa.func.arrayMap(
                    Lambda(lambda x: sa.cast(sa.func.ifNull(sa.func.toString(x), null_str), ch_types.String)),
                    array
                ),
                sep
            )
        ),
    ]),

    # arr_sum
    base.FuncArraySumFloat(variants=[
        V(D.and_above(D.CLICKHOUSE_21_8), sa.func.arraySum),
    ]),
    base.FuncArraySumInt(variants=[
        V(D.and_above(D.CLICKHOUSE_21_8), sa.func.arraySum),
    ]),

    # array
    base.FuncConstArrayFloat.for_dialect(D.CLICKHOUSE),
    base.FuncConstArrayInt.for_dialect(D.CLICKHOUSE),
    base.FuncConstArrayStr.for_dialect(D.CLICKHOUSE),
    base.FuncNonConstArrayInt(variants=[
        V(D.CLICKHOUSE, lambda *args: sa.func.array(*args)),
    ]),
    base.FuncNonConstArrayFloat(variants=[
        V(D.CLICKHOUSE, lambda *args: sa.func.array(*args)),
    ]),
    base.FuncNonConstArrayStr(variants=[
        V(D.CLICKHOUSE, lambda *args: sa.func.array(*args)),
    ]),

    # cast_arr_float
    base.FuncFloatArrayFromIntArray(variants=[
        V(D.CLICKHOUSE, lambda array: sa.func.arrayMap(
            Lambda(lambda x: sa.func.toFloat64(x)),
            array
        )),
    ]),
    base.FuncFloatArrayFromFloatArray.for_dialect(D.CLICKHOUSE),
    base.FuncFloatArrayFromStringArray(variants=[
        V(D.CLICKHOUSE, lambda array: sa.func.arrayMap(
            Lambda(lambda x: sa.func.toFloat64OrNull(x)),
            array
        )),
    ]),

    # cast_arr_int
    base.FuncIntArrayFromIntArray.for_dialect(D.CLICKHOUSE),
    base.FuncIntArrayFromFloatArray(variants=[
        V(D.CLICKHOUSE, lambda array: sa.func.arrayMap(
            Lambda(lambda x: sa.func.toInt64(sa.func.floor(x))),
            array
        )),
    ]),
    base.FuncIntArrayFromStringArray(variants=[
        V(D.CLICKHOUSE, lambda array: sa.func.arrayMap(
            Lambda(lambda x: sa.func.toInt64OrNull(x)),
            array
        )),
    ]),

    # cast_arr_str
    base.FuncStringArrayFromIntArray(variants=[
        V(D.CLICKHOUSE, lambda array: sa.func.arrayMap(
            Lambda(lambda x: sa.func.toString(x)),
            array
        )),
    ]),
    base.FuncStringArrayFromFloatArray(variants=[
        V(D.CLICKHOUSE, lambda array: sa.func.arrayMap(
            Lambda(lambda x: sa.func.toString(x)),
            array
        )),
    ]),
    base.FuncStringArrayFromStringArray.for_dialect(D.CLICKHOUSE),

    # contains
    base.FuncArrayContains(variants=[
        V(D.CLICKHOUSE, sa.func.has),
    ]),

    # contains_all
    base.FuncArrayContainsAll(variants=[
        V(D.CLICKHOUSE, sa.func.hasAll),
    ]),

    # contains_any
    base.FuncArrayContainsAny(variants=[
        V(D.CLICKHOUSE, sa.func.hasAny),
    ]),

    # contains_subsequence
    base.FuncArrayContainsSubsequence(variants=[
        V(D.and_above(D.CLICKHOUSE_21_8), sa.func.hasSubstr),
    ]),

    # count_item
    base.FuncArrayCountItemInt(variants=[
        V(D.CLICKHOUSE, sa.func.countEqual),
    ]),
    base.FuncArrayCountItemFloat(variants=[
        V(D.CLICKHOUSE, sa.func.countEqual),
    ]),
    base.FuncArrayCountItemStr(variants=[
        V(D.CLICKHOUSE, sa.func.countEqual),
    ]),

    # get_item
    base.FuncGetArrayItemFloat(variants=[
        V(D.CLICKHOUSE, sa.func.arrayElement),
    ]),
    base.FuncGetArrayItemInt(variants=[
        V(D.CLICKHOUSE, sa.func.arrayElement),
    ]),
    base.FuncGetArrayItemStr(variants=[
        V(D.CLICKHOUSE, sa.func.arrayElement),
    ]),

    # len
    base.FuncLenArray(variants=[
        V(D.CLICKHOUSE, sa.func.length),
    ]),

    # replace
    base.FuncReplaceArrayLiteralNull(variants=[
        V(D.CLICKHOUSE, lambda array, old, new: sa.func.arrayMap(
            Lambda(lambda x: sa.func.ifNull(x, new)),
            array
        )),
    ]),
    base.FuncReplaceArrayDefault(variants=[
        V(D.CLICKHOUSE, lambda array, old, new: sa.func.arrayMap(
            Lambda(lambda x: sa.func.if_(x == old, new, x)),
            array
        )),
    ]),

    # slice
    base.FuncArraySlice(variants=[
        V(D.CLICKHOUSE, sa.func.arraySlice),
    ]),

    # startswith
    base.FuncStartswithArrayConst(variants=[
        V(D.CLICKHOUSE, lambda x, y, _env: sa.func.arraySlice(x, 1, len(un_literal(y))) == y),
    ]),
    base.FuncStartswithArrayNonConst(variants=[
        V(D.CLICKHOUSE, lambda x, y, _env: sa.func.arraySlice(x, 1, sa.func.length(y)) == y),
    ]),

    # unnest
    base.FuncUnnestArrayFloat(variants=[
        V(D.CLICKHOUSE, sa.func.arrayJoin),
    ]),
    base.FuncUnnestArrayInt(variants=[
        V(D.CLICKHOUSE, sa.func.arrayJoin),
    ]),
    base.FuncUnnestArrayStr(variants=[
        V(D.CLICKHOUSE, sa.func.arrayJoin),
    ]),
]
