import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_string as base
from dl_formula.shortcuts import n

from dl_connector_bigquery.formula.constants import BigQueryDialect as D

V = TranslationVariant.make


DEFINITIONS_STRING = [
    # ascii
    base.FuncAscii.for_dialect(D.BIGQUERY),
    # char
    base.FuncChar(
        variants=[
            V(D.BIGQUERY, sa.func.CHR),
        ]
    ),
    # concat
    base.Concat1.for_dialect(D.BIGQUERY),
    base.ConcatMultiStrConst.for_dialect(D.BIGQUERY),
    base.ConcatMultiStr.for_dialect(D.BIGQUERY),
    base.ConcatMultiAny.for_dialect(D.BIGQUERY),
    # contains
    base.FuncContainsNonConst(
        variants=[
            V(D.BIGQUERY, lambda x, y: sa.func.STRPOS(x, y) != 0),
        ]
    ),
    base.FuncContainsNonString.for_dialect(D.BIGQUERY),
    # endswith
    base.FuncEndswithNonConst(variants=[V(D.BIGQUERY, sa.func.ENDS_WITH)]),
    base.FuncEndswithNonString.for_dialect(D.BIGQUERY),
    # find
    base.FuncFind2(variants=[V(D.BIGQUERY, sa.func.STRPOS)]),
    base.FuncFind3(
        variants=[
            V(
                D.BIGQUERY,
                lambda x, y, z: n.if_(sa.func.STRPOS(sa.func.SUBSTRING(x, z), y) > 0)  # type: ignore
                .then(sa.func.STRPOS(sa.func.SUBSTRING(x, z), y) + z - 1)
                .else_(0),
            ),
        ]
    ),
    # icontains
    base.FuncIContainsNonConst.for_dialect(D.BIGQUERY),
    base.FuncIContainsNonString.for_dialect(D.BIGQUERY),
    # iendswith
    base.FuncIEndswithNonConst.for_dialect(D.BIGQUERY),
    base.FuncIEndswithNonString.for_dialect(D.BIGQUERY),
    # istartswith
    base.FuncIStartswithNonConst.for_dialect(D.BIGQUERY),
    base.FuncIStartswithNonString.for_dialect(D.BIGQUERY),
    # left
    base.FuncLeft.for_dialect(D.BIGQUERY),
    # len
    base.FuncLenString(variants=[V(D.BIGQUERY, sa.func.LENGTH)]),
    # lower
    base.FuncLowerConst.for_dialect(D.BIGQUERY),
    base.FuncLowerNonConst.for_dialect(D.BIGQUERY),
    # ltrim
    base.FuncLtrim(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.LTRIM(value, " ")),
        ]
    ),
    # regexp_extract
    base.FuncRegexpExtract(variants=[V(D.BIGQUERY, sa.func.REGEXP_EXTRACT)]),
    # regexp_extract_nth
    base.FuncRegexpExtractNth(
        variants=[
            V(
                D.BIGQUERY,
                lambda text, pattern, ind: sa.type_coerce(
                    sa.func.REGEXP_EXTRACT_ALL(text, pattern), sa.ARRAY(sa.String)
                )[ind - 1],
            ),
        ]
    ),
    # regexp_match
    base.FuncRegexpMatch(variants=[V(D.BIGQUERY, sa.func.REGEXP_CONTAINS)]),
    # regexp_replace
    base.FuncRegexpReplace(variants=[V(D.BIGQUERY, sa.func.REGEXP_REPLACE)]),
    # replace
    base.FuncReplace.for_dialect(D.BIGQUERY),
    # right
    base.FuncRight.for_dialect(D.BIGQUERY),
    # rtrim
    base.FuncRtrim(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.RTRIM(value, " ")),
        ]
    ),
    # space
    base.FuncSpaceConst.for_dialect(D.BIGQUERY),
    base.FuncSpaceNonConst(variants=[V(D.BIGQUERY, lambda value: sa.func.REPEAT(" ", value))]),
    # split
    # base.FuncSplit1,  # FIXME: Add array support
    # base.FuncSplit2,  # FIXME: Add array support
    base.FuncSplit3(
        variants=[
            V(
                D.BIGQUERY,
                lambda text, delim, ind: (sa.type_coerce(sa.func.SPLIT(text, delim), sa.ARRAY(sa.String))[ind - 1]),
            ),
        ]
    ),
    # startswith
    base.FuncStartswithNonConst(variants=[V(D.BIGQUERY, sa.func.STARTS_WITH)]),
    base.FuncStartswithNonString.for_dialect(D.BIGQUERY),
    # substr
    base.FuncSubstr2.for_dialect(D.BIGQUERY),
    base.FuncSubstr3.for_dialect(D.BIGQUERY),
    # trim
    base.FuncTrim(
        variants=[
            V(D.BIGQUERY, lambda value: sa.func.TRIM(value, " ")),
        ]
    ),
    # upper
    base.FuncUpperConst.for_dialect(D.BIGQUERY),
    base.FuncUpperNonConst.for_dialect(D.BIGQUERY),
]
