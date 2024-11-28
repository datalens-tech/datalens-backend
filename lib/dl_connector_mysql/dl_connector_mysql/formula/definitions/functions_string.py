import sqlalchemy as sa

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import dl_formula.definitions.functions_string as base
from dl_formula.shortcuts import n

from dl_connector_mysql.formula.constants import MySQLDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


DEFINITIONS_STRING = [
    # ascii
    base.FuncAscii.for_dialect(D.MYSQL),
    # char
    base.FuncChar.for_dialect(D.MYSQL),
    # concat
    base.Concat1.for_dialect((D.MYSQL)),
    base.ConcatMultiStrConst.for_dialect(D.MYSQL),
    base.ConcatMultiStr.for_dialect(D.MYSQL),
    base.ConcatMultiAny.for_dialect(D.MYSQL),
    # contains
    base.FuncContainsConst.for_dialect(D.MYSQL),
    base.FuncContainsNonConst(
        variants=[
            V(D.MYSQL, lambda x, y: sa.func.LOCATE(y, x) > 0),
        ]
    ),
    base.FuncContainsNonString.for_dialect(D.MYSQL),
    # notcontains
    base.FuncNotContainsConst.for_dialect(D.MYSQL),
    base.FuncNotContainsNonConst.for_dialect(D.MYSQL),
    base.FuncNotContainsNonString.for_dialect(D.MYSQL),
    # endswith
    base.FuncEndswithConst.for_dialect(D.MYSQL),
    base.FuncEndswithNonConst(
        variants=[
            V(D.MYSQL, lambda x, y: (sa.func.SUBSTRING(x, sa.func.char_length(x) - sa.func.char_length(y) + 1) == y)),
        ]
    ),
    base.FuncEndswithNonString.for_dialect(D.MYSQL),
    # find
    base.FuncFind2(
        variants=[
            V(D.MYSQL, lambda x, y: sa.func.LOCATE(y, x)),
        ]
    ),
    base.FuncFind3(
        variants=[
            V(
                D.MYSQL,
                lambda x, y, z: n.if_(sa.func.LOCATE(y, sa.func.SUBSTRING(x, z)) > 0)  # type: ignore  # TODO: fix
                .then(sa.func.LOCATE(y, sa.func.SUBSTRING(x, z)) + z - 1)
                .else_(0),
            ),
        ]
    ),
    # icontains
    base.FuncIContainsNonConst.for_dialect(D.MYSQL),
    base.FuncIContainsNonString.for_dialect(D.MYSQL),
    # iendswith
    base.FuncIEndswithNonConst.for_dialect(D.MYSQL),
    base.FuncIEndswithNonString.for_dialect(D.MYSQL),
    # istartswith
    base.FuncIStartswithNonConst.for_dialect(D.MYSQL),
    base.FuncIStartswithNonString.for_dialect(D.MYSQL),
    # left
    base.FuncLeft.for_dialect(D.MYSQL),
    # len
    base.FuncLenString(
        variants=[
            V(D.MYSQL, sa.func.char_length),
        ]
    ),
    # lower
    base.FuncLowerConst.for_dialect(D.MYSQL),
    base.FuncLowerNonConst.for_dialect(D.MYSQL),
    # ltrim
    base.FuncLtrim.for_dialect(D.MYSQL),
    # regexp_extract
    base.FuncRegexpExtract(
        variants=[
            V(D.and_above(D.MYSQL_8_0_40), sa.func.REGEXP_SUBSTR),
        ]
    ),
    # regexp_extract_nth
    base.FuncRegexpExtractNth(
        variants=[
            V(D.and_above(D.MYSQL_8_0_40), lambda text, pattern, ind: sa.func.REGEXP_SUBSTR(text, pattern, 1, ind)),
        ]
    ),
    # regexp_match
    base.FuncRegexpMatch(
        variants=[
            V(D.MYSQL, lambda text, pattern: text.op("REGEXP")(pattern)),
        ]
    ),
    # regexp_replace
    base.FuncRegexpReplace(
        variants=[
            V(D.and_above(D.MYSQL_8_0_40), sa.func.REGEXP_REPLACE),
        ]
    ),
    # replace
    base.FuncReplace.for_dialect(D.MYSQL),
    # right
    base.FuncRight.for_dialect(D.MYSQL),
    # rtrim
    base.FuncRtrim.for_dialect(D.MYSQL),
    # space
    base.FuncSpaceConst.for_dialect(D.MYSQL),
    base.FuncSpaceNonConst.for_dialect(D.MYSQL),
    # split
    base.FuncSplit3(
        variants=[
            V(
                D.MYSQL,
                lambda text, delim, ind: (
                    sa.func.SUBSTRING_INDEX(sa.func.SUBSTRING_INDEX(text, delim, ind), delim, -sa.func.SIGN(ind))
                ),
            ),
        ]
    ),
    # startswith
    base.FuncStartswithConst.for_dialect(D.MYSQL),
    base.FuncStartswithNonConst(
        variants=[
            V(D.MYSQL, lambda x, y: sa.func.LOCATE(y, x) == 1),
        ]
    ),
    base.FuncStartswithNonString.for_dialect(D.MYSQL),
    # substr
    base.FuncSubstr2.for_dialect(D.MYSQL),
    base.FuncSubstr3.for_dialect(D.MYSQL),
    # trim
    base.FuncTrim.for_dialect(D.MYSQL),
    # upper
    base.FuncUpperConst.for_dialect(D.MYSQL),
    base.FuncUpperNonConst.for_dialect(D.MYSQL),
]
