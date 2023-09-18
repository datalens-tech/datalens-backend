import sqlalchemy as sa

from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D
from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import dl_formula.definitions.functions_string as base
from dl_formula.shortcuts import n

V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class FuncIEndswithNonConstSF(base.FuncIEndswithNonConst):
    variants = [
        VW(
            D.SNOWFLAKE,
            lambda s, substr: n.func.ENDSWITH(n.func.LOWER(s), n.func.LOWER(substr)),
        ),
    ]


DEFINITIONS_STRING = [
    # ascii
    base.FuncAscii.for_dialect(D.SNOWFLAKE),
    # char
    base.FuncChar(
        variants=[
            V(D.SNOWFLAKE, sa.func.CHR),
        ]
    ),
    # concat
    base.Concat1.for_dialect(D.SNOWFLAKE),
    base.ConcatMultiStrConst.for_dialect(D.SNOWFLAKE),
    base.ConcatMultiStr.for_dialect(D.SNOWFLAKE),
    base.ConcatMultiAny.for_dialect(D.SNOWFLAKE),
    # contains
    base.FuncContainsNonConst(
        variants=[
            V(D.SNOWFLAKE, lambda x, y: sa.func.CONTAINS(x, y)),
        ]
    ),
    base.FuncContainsNonString.for_dialect(D.SNOWFLAKE),
    # endswith
    base.FuncEndswithNonString.for_dialect(D.SNOWFLAKE),
    base.FuncEndswithNonConst(variants=[V(D.SNOWFLAKE, sa.func.ENDSWITH)]),
    # find
    base.FuncFind2(variants=[V(D.SNOWFLAKE, lambda string, substring: sa.func.POSITION(substring, string))]),
    base.FuncFind3(
        variants=[V(D.SNOWFLAKE, lambda string, substring, start: sa.func.POSITION(substring, string, start))]
    ),
    # icontains
    base.FuncIContainsNonConst.for_dialect(D.SNOWFLAKE),
    base.FuncIContainsNonString.for_dialect(D.SNOWFLAKE),
    # iendswith
    base.FuncIEndswithNonConst.for_dialect(D.SNOWFLAKE),
    base.FuncIEndswithNonString.for_dialect(D.SNOWFLAKE),
    # istartswith
    base.FuncIStartswithNonConst.for_dialect(D.SNOWFLAKE),
    base.FuncIStartswithNonString.for_dialect(D.SNOWFLAKE),
    # left
    base.FuncLeft.for_dialect(D.SNOWFLAKE),
    # len
    base.FuncLenString(variants=[V(D.SNOWFLAKE, sa.func.LENGTH)]),
    # lower
    base.FuncLowerConst.for_dialect(D.SNOWFLAKE),
    base.FuncLowerNonConst.for_dialect(D.SNOWFLAKE),
    # ltrim
    base.FuncLtrim(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.LTRIM(value, " ")),
        ]
    ),
    # regexp_extract
    base.FuncRegexpExtract(variants=[V(D.SNOWFLAKE, sa.func.REGEXP_SUBSTR)]),
    # regexp_extract_nth
    base.FuncRegexpExtractNth(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda subj, pattern, nth: sa.func.REGEXP_SUBSTR(subj, pattern, 1, nth),
            ),
        ]
    ),
    # regexp_match
    base.FuncRegexpMatch(
        variants=[
            # todo: has a native method, but it should be a bin op, but SA seems to translate into func call
            #   maybe dig in and fix at some point
            V(D.SNOWFLAKE, lambda text, pattern: sa.func.REGEXP_SUBSTR(text, pattern).isnot(None)),
        ]
    ),
    # regexp_replace
    base.FuncRegexpReplace(variants=[V(D.SNOWFLAKE, sa.func.REGEXP_REPLACE)]),
    # replace
    base.FuncReplace.for_dialect(D.SNOWFLAKE),
    # right
    base.FuncRight.for_dialect(D.SNOWFLAKE),
    # rtrim
    base.FuncRtrim(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.RTRIM(value, " ")),
        ]
    ),
    # space
    base.FuncSpaceConst.for_dialect(D.SNOWFLAKE),
    base.FuncSpaceNonConst(variants=[V(D.SNOWFLAKE, lambda value: sa.func.REPEAT(" ", value))]),
    # split
    # base.FuncSplit1,  # FIXME: Add array support
    # base.FuncSplit2,  # FIXME: Add array support
    base.FuncSplit3(
        variants=[
            V(D.SNOWFLAKE, sa.func.SPLIT_PART),
        ]
    ),
    # startswith
    base.FuncStartswithNonConst(variants=[V(D.SNOWFLAKE, sa.func.STARTSWITH)]),
    base.FuncStartswithNonString.for_dialect(D.SNOWFLAKE),
    # substr
    base.FuncSubstr2.for_dialect(D.SNOWFLAKE),
    base.FuncSubstr3.for_dialect(D.SNOWFLAKE),
    # trim
    base.FuncTrim(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TRIM(value, " ")),
        ]
    ),
    # upper
    base.FuncUpperConst.for_dialect(D.SNOWFLAKE),
    base.FuncUpperNonConst.for_dialect(D.SNOWFLAKE),
]
