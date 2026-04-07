import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_string as base

from dl_connector_starrocks.formula.constants import StarRocksDialect as D


V = TranslationVariant.make


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
    # TODO: BI-7171 FuncContainsConst (StarRocks doesn't support LIKE ESCAPE)
    base.FuncContainsNonConst(
        variants=[
            V(D.STARROCKS, lambda x, y: sa.func.LOCATE(y, x) > 0),
        ]
    ),
    base.FuncContainsNonString.for_dialect(D.STARROCKS),
    # notcontains
    # TODO: BI-7171 FuncNotContainsConst (StarRocks doesn't support LIKE ESCAPE)
    base.FuncNotContainsNonConst.for_dialect(D.STARROCKS),
    base.FuncNotContainsNonString.for_dialect(D.STARROCKS),
    # endswith
    # TODO: BI-7171 FuncEndswithConst, FuncEndswithNonConst (SUBSTRING length arithmetic)
    base.FuncEndswithNonString.for_dialect(D.STARROCKS),
    # find
    base.FuncFind2(
        variants=[
            V(D.STARROCKS, lambda x, y: sa.func.LOCATE(y, x)),
        ]
    ),
    # TODO: BI-7171 FuncFind3
    # icontains
    # TODO: BI-7171 FuncIContainsConst (StarRocks doesn't support LIKE ESCAPE)
    base.FuncIContainsNonConst.for_dialect(D.STARROCKS),
    base.FuncIContainsNonString.for_dialect(D.STARROCKS),
    # iendswith
    # TODO: BI-7171 FuncIEndswithConst (StarRocks doesn't support LIKE ESCAPE)
    base.FuncIEndswithNonConst.for_dialect(D.STARROCKS),
    base.FuncIEndswithNonString.for_dialect(D.STARROCKS),
    # istartswith
    # TODO: BI-7171 FuncIStartswithConst (StarRocks doesn't support LIKE ESCAPE)
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
            V(D.STARROCKS, lambda text, pattern: sa.func.regexp_extract(text, pattern, 0)),
        ]
    ),
    # TODO: BI-7171 FuncRegexpExtractNth
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
            V(D.STARROCKS, lambda text, delim, ind: sa.func.SPLIT_PART(text, delim, ind)),
        ]
    ),
    # startswith
    # TODO: BI-7171 FuncStartswithConst (StarRocks doesn't support LIKE ESCAPE)
    base.FuncStartswithNonConst(
        variants=[
            V(D.STARROCKS, lambda x, y: sa.func.STARTS_WITH(x, y)),
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
