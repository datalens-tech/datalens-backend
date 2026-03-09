import sqlalchemy as sa

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import dl_formula.definitions.functions_string as base
from dl_formula.shortcuts import n

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


DEFINITIONS_STRING = [
    # ascii
    base.FuncAscii.for_dialect(D.STARROCKS),
    # char
    base.FuncChar.for_dialect(D.STARROCKS),
    # concat
    base.Concat1.for_dialect(D.STARROCKS),
    base.ConcatMultiStrConst.for_dialect(D.STARROCKS),
    base.ConcatMultiStr.for_dialect(D.STARROCKS),
    base.ConcatMultiAny.for_dialect(D.STARROCKS),
    # contains
    base.FuncContainsConst.for_dialect(D.STARROCKS),
    base.FuncContainsNonConst(
        variants=[
            V(D.STARROCKS, lambda x, y: sa.func.LOCATE(y, x) > 0),
        ]
    ),
    base.FuncContainsNonString.for_dialect(D.STARROCKS),
    # notcontains
    base.FuncNotContainsConst.for_dialect(D.STARROCKS),
    base.FuncNotContainsNonConst.for_dialect(D.STARROCKS),
    base.FuncNotContainsNonString.for_dialect(D.STARROCKS),
    # endswith
    base.FuncEndswithConst.for_dialect(D.STARROCKS),
    base.FuncEndswithNonConst(
        variants=[
            V(
                D.STARROCKS,
                lambda x, y: (sa.func.SUBSTRING(x, sa.func.char_length(x) - sa.func.char_length(y) + 1) == y),
            ),
        ]
    ),
    base.FuncEndswithNonString.for_dialect(D.STARROCKS),
    # find
    base.FuncFind2(
        variants=[
            V(D.STARROCKS, lambda x, y: sa.func.LOCATE(y, x)),
        ]
    ),
    base.FuncFind3(
        variants=[
            V(
                D.STARROCKS,
                lambda x, y, z: n.if_(sa.func.LOCATE(y, sa.func.SUBSTRING(x, z)) > 0)  # type: ignore  # TODO: fix
                .then(sa.func.LOCATE(y, sa.func.SUBSTRING(x, z)) + z - 1)
                .else_(0),
            ),
        ]
    ),
    # icontains
    base.FuncIContainsNonConst.for_dialect(D.STARROCKS),
    base.FuncIContainsNonString.for_dialect(D.STARROCKS),
    # iendswith
    base.FuncIEndswithNonConst.for_dialect(D.STARROCKS),
    base.FuncIEndswithNonString.for_dialect(D.STARROCKS),
    # istartswith
    base.FuncIStartswithNonConst.for_dialect(D.STARROCKS),
    base.FuncIStartswithNonString.for_dialect(D.STARROCKS),
    # left
    base.FuncLeft.for_dialect(D.STARROCKS),
    # len
    base.FuncLenString(
        variants=[
            V(D.STARROCKS, sa.func.char_length),
        ]
    ),
    # lower
    base.FuncLowerConst.for_dialect(D.STARROCKS),
    base.FuncLowerNonConst.for_dialect(D.STARROCKS),
    # ltrim
    base.FuncLtrim.for_dialect(D.STARROCKS),
    # regexp_extract
    base.FuncRegexpExtract(
        variants=[
            V(D.STARROCKS, sa.func.REGEXP_SUBSTR),
        ]
    ),
    # regexp_extract_nth
    base.FuncRegexpExtractNth(
        variants=[
            V(D.STARROCKS, lambda text, pattern, ind: sa.func.REGEXP_SUBSTR(text, pattern, 1, ind)),
        ]
    ),
    # regexp_match
    base.FuncRegexpMatch(
        variants=[
            V(D.STARROCKS, lambda text, pattern: text.op("REGEXP")(pattern)),
        ]
    ),
    # regexp_replace
    base.FuncRegexpReplace(
        variants=[
            V(D.STARROCKS, sa.func.REGEXP_REPLACE),
        ]
    ),
    # replace
    base.FuncReplace.for_dialect(D.STARROCKS),
    # right
    base.FuncRight.for_dialect(D.STARROCKS),
    # rtrim
    base.FuncRtrim.for_dialect(D.STARROCKS),
    # space
    base.FuncSpaceConst.for_dialect(D.STARROCKS),
    base.FuncSpaceNonConst.for_dialect(D.STARROCKS),
    # split
    base.FuncSplit3(
        variants=[
            V(
                D.STARROCKS,
                lambda text, delim, ind: (
                    sa.func.SUBSTRING_INDEX(sa.func.SUBSTRING_INDEX(text, delim, ind), delim, -sa.func.SIGN(ind))
                ),
            ),
        ]
    ),
    # startswith
    base.FuncStartswithConst.for_dialect(D.STARROCKS),
    base.FuncStartswithNonConst(
        variants=[
            V(D.STARROCKS, lambda x, y: sa.func.LOCATE(y, x) == 1),
        ]
    ),
    base.FuncStartswithNonString.for_dialect(D.STARROCKS),
    # substr
    base.FuncSubstr2.for_dialect(D.STARROCKS),
    base.FuncSubstr3.for_dialect(D.STARROCKS),
    # trim
    base.FuncTrim.for_dialect(D.STARROCKS),
    # upper
    base.FuncUpperConst.for_dialect(D.STARROCKS),
    base.FuncUpperNonConst.for_dialect(D.STARROCKS),
]
