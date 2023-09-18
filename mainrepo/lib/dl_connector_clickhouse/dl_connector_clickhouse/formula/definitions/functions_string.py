import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_string as base
from dl_formula.shortcuts import n

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D

V = TranslationVariant.make


DEFINITIONS_STRING = [
    # ascii
    base.FuncAscii(
        variants=[
            V(D.CLICKHOUSE, sa.func.reinterpretAsUInt8),
        ]
    ),
    # char
    base.FuncChar(
        variants=[
            V(D.CLICKHOUSE, sa.func.reinterpretAsString),
        ]
    ),
    # concat
    base.Concat1.for_dialect((D.CLICKHOUSE)),
    base.ConcatMultiStrConst.for_dialect(D.CLICKHOUSE),
    base.ConcatMultiStr(
        variants=[
            V(D.CLICKHOUSE, lambda *args: sa.func.concat(*args)),
        ]
    ),
    base.ConcatMultiAny.for_dialect(D.CLICKHOUSE),
    # contains
    base.FuncContainsConst(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda x, y: x.like(
                    "%{}%".format(base.quote_like(y.value)),
                ),
            ),
        ]
    ),
    base.FuncContainsNonConst(
        variants=[
            V(D.CLICKHOUSE, lambda x, y: sa.func.position(x, y) > 0),
        ]
    ),
    base.FuncContainsNonString.for_dialect(D.CLICKHOUSE),
    # endswith
    base.FuncEndswithNonConst(
        variants=[
            V(D.CLICKHOUSE, lambda x, y: sa.func.endsWith(x, y)),
        ]
    ),
    base.FuncEndswithNonString.for_dialect(D.CLICKHOUSE),
    # find
    base.FuncFind2(
        variants=[
            V(D.CLICKHOUSE, sa.func.positionUTF8),
        ]
    ),
    base.FuncFind3(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda x, y, z: n.if_(  # type: ignore  # TODO: fix
                    sa.func.positionUTF8(sa.func.substringUTF8(x, z, sa.func.lengthUTF8(x)), y) > 0
                )
                .then(sa.func.positionUTF8(sa.func.substringUTF8(x, z, sa.func.lengthUTF8(x)), y) + z - 1)
                .else_(0),
            ),
        ]
    ),
    # icontains
    base.FuncIContainsConst(
        variants=[
            V(D.CLICKHOUSE, lambda x, y: sa.func.positionCaseInsensitiveUTF8(x, y) != 0),
        ]
    ),
    base.FuncIContainsNonConst(
        variants=[
            V(D.CLICKHOUSE, lambda x, y: sa.func.positionCaseInsensitiveUTF8(x, y) != 0),
        ]
    ),
    base.FuncIContainsNonString.for_dialect(D.CLICKHOUSE),
    # iendswith
    base.FuncIEndswithNonConst.for_dialect(D.CLICKHOUSE),
    base.FuncIEndswithNonString.for_dialect(D.CLICKHOUSE),
    # istartswith
    base.FuncIStartswithNonConst.for_dialect(D.CLICKHOUSE),
    base.FuncIStartswithNonString.for_dialect(D.CLICKHOUSE),
    # left
    base.FuncLeft(
        variants=[
            V(D.CLICKHOUSE, lambda x, y: sa.func.substringUTF8(x, 1, y)),
        ]
    ),
    # len
    base.FuncLenString(
        variants=[
            V(D.CLICKHOUSE, sa.func.lengthUTF8),
        ]
    ),
    # lower
    base.FuncLowerConst.for_dialect(D.CLICKHOUSE),
    base.FuncLowerNonConst(
        variants=[
            V(D.CLICKHOUSE, sa.func.lowerUTF8),
        ]
    ),
    # ltrim
    base.FuncLtrim(
        variants=[
            V(D.CLICKHOUSE, sa.func.trimLeft),
        ]
    ),
    # regexp_extract
    base.FuncRegexpExtract(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda text, pattern: sa.func.arrayElement(sa.func.extractAll(sa.func.assumeNotNull(text), pattern), 1),
            ),
        ]
    ),
    # regexp_extract_nth
    base.FuncRegexpExtractNth(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda text, pattern, ind: sa.func.arrayElement(
                    sa.func.extractAll(sa.func.assumeNotNull(text), pattern), ind
                ),
            ),
        ]
    ),
    # regexp_match
    base.FuncRegexpMatch(
        variants=[
            V(D.CLICKHOUSE, sa.func.match),
        ]
    ),
    # regexp_replace
    base.FuncRegexpReplace(
        variants=[
            V(D.CLICKHOUSE, sa.func.replaceRegexpAll),
        ]
    ),
    # replace
    base.FuncReplace(
        variants=[
            V(D.CLICKHOUSE, sa.func.replaceAll),
        ]
    ),
    # right
    base.FuncRight(
        variants=[
            V(D.CLICKHOUSE, lambda x, y: sa.func.substringUTF8(x, sa.func.lengthUTF8(x) - y + 1, y)),
        ]
    ),
    # rtrim
    base.FuncRtrim(
        variants=[
            V(D.CLICKHOUSE, sa.func.trimRight),
        ]
    ),
    # space
    base.FuncSpaceConst.for_dialect(D.CLICKHOUSE),
    base.FuncSpaceNonConst(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda size: sa.func.arrayStringConcat(sa.func.arrayResize(sa.func.emptyArrayString(), size, " ")),
            ),
        ]
    ),
    # split
    base.FuncSplit1(
        variants=[
            V(D.CLICKHOUSE, lambda text: sa.func.splitByString(",", sa.func.assumeNotNull(text))),
        ]
    ),
    base.FuncSplit2(
        variants=[
            V(D.CLICKHOUSE, lambda text, delim: sa.func.splitByString(delim, sa.func.assumeNotNull(text))),
        ]
    ),
    base.FuncSplit3(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda text, delim, ind: sa.func.arrayElement(
                    sa.func.splitByString(delim, sa.func.assumeNotNull(text)), ind
                ),
            ),
        ]
    ),
    # startswith
    base.FuncStartswithNonConst(
        variants=[
            V(D.CLICKHOUSE, lambda x, y: sa.func.startsWith(x, y)),
        ]
    ),
    base.FuncStartswithNonString.for_dialect(D.CLICKHOUSE),
    # substr
    base.FuncSubstr2(
        variants=[
            V(D.CLICKHOUSE, lambda text, start: sa.func.substringUTF8(text, start, sa.func.lengthUTF8(text))),
        ]
    ),
    base.FuncSubstr3(
        variants=[
            V(D.CLICKHOUSE, lambda text, start, length: sa.func.substringUTF8(text, start, length)),
        ]
    ),
    # trim
    base.FuncTrim(
        variants=[
            V(D.CLICKHOUSE, sa.func.trimBoth),
        ]
    ),
    # upper
    base.FuncUpperConst.for_dialect(D.CLICKHOUSE),
    base.FuncUpperNonConst(
        variants=[
            V(D.CLICKHOUSE, sa.func.upperUTF8),
        ]
    ),
    # utf8
    base.FuncUtf8(
        variants=[
            V(D.CLICKHOUSE, lambda text, charset: sa.func.convertCharset(text, charset, "UTF-8")),
        ]
    ),
]
