import sqlalchemy as sa

from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
import dl_formula.definitions.functions_string as base
from dl_formula.shortcuts import n

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


DEFINITIONS_STRING = [
    # ascii
    # base.FuncAscii.for_dialect(D.TRINO),
    # char
    # base.FuncChar(
    #     variants=[
    #         V(D.TRINO, sa.func.CHR),
    #     ]
    # ),
    # concat
    # base.Concat1.for_dialect((D.TRINO)),
    # base.ConcatMultiStrConst.for_dialect(D.TRINO),
    base.ConcatMultiStr(
        variants=[
            V(
                D.TRINO,
                lambda *args: sa.func.concat_ws("", *args),
            ),
        ]
    ),
    base.ConcatMultiAny(
        variants=[
            VW(
                D.TRINO,
                lambda *args: n.func.CONCAT(*(n.func.STR(arg) for arg in args)),
            ),
        ]
    ),
    # contains
    # base.FuncContainsConst.for_dialect(D.TRINO),
    # base.FuncContainsNonConst(
    #     variants=[
    #         V(D.TRINO, lambda x, y: sa.func.STRPOS(x, y) > 0),
    #     ]
    # ),
    # base.FuncContainsNonString.for_dialect(D.TRINO),
    # notcontains
    # base.FuncNotContainsConst.for_dialect(D.TRINO),
    # base.FuncNotContainsNonConst.for_dialect(D.TRINO),
    # base.FuncNotContainsNonString.for_dialect(D.TRINO),
    # endswith
    # base.FuncEndswithConst.for_dialect(D.TRINO),
    # base.FuncEndswithNonConst(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda x, y: (sa.func.SUBSTRING(x, sa.func.char_length(x) - sa.func.char_length(y) + 1) == y),
    #         ),
    #     ]
    # ),
    # base.FuncEndswithNonString.for_dialect(D.TRINO),
    # find
    # base.FuncFind2(
    #     variants=[
    #         V(D.TRINO, sa.func.STRPOS),
    #     ]
    # ),
    # base.FuncFind3(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda x, y, z: n.if_(sa.func.STRPOS(sa.func.SUBSTRING(x, z), y) > 0)  # type: ignore  # TODO: fix
    #             .then(
    #                 sa.func.STRPOS(sa.func.SUBSTRING(x, z), y) + z - 1,
    #             )
    #             .else_(0),
    #         ),
    #     ]
    # ),
    # icontains
    # base.FuncIContainsConst.for_dialect(D.TRINO),
    # base.FuncIContainsNonConst.for_dialect(D.TRINO),
    # base.FuncIContainsNonString.for_dialect(D.TRINO),
    # iendswith
    # base.FuncIEndswithConst.for_dialect(D.TRINO),
    # base.FuncIEndswithNonConst.for_dialect(D.TRINO),
    # base.FuncIEndswithNonString.for_dialect(D.TRINO),
    # istartswith
    # base.FuncIStartswithConst.for_dialect(D.TRINO),
    # base.FuncIStartswithNonConst.for_dialect(D.TRINO),
    # base.FuncIStartswithNonString.for_dialect(D.TRINO),
    # left
    # base.FuncLeft.for_dialect(D.TRINO),
    # len
    # base.FuncLenString(
    #     variants=[
    #         V(D.TRINO, sa.func.LENGTH),
    #     ]
    # ),
    # lower
    # base.FuncLowerConst.for_dialect(D.TRINO),
    # base.FuncLowerNonConst.for_dialect(D.TRINO),
    # ltrim
    # base.FuncLtrim.for_dialect(D.TRINO),
    # regexp_extract
    # base.FuncRegexpExtract(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda text, pattern: sa.type_coerce(
    #                 sa.select([sa.func.REGEXP_MATCHES(text, pattern, "g")]).limit(1).as_scalar(),
    #                 sa_postgresql.ARRAY(sa.String),
    #             )[1],
    #         ),
    #     ]
    # ),
    # regexp_extract_all
    # base.FuncRegexpExtractAll(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             regexp_matches_in_brackets,
    #         )
    #     ]
    # ),
    # regexp_extract_nth
    # base.FuncRegexpExtractNth(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda text, pattern, ind: sa.type_coerce(
    #                 sa.select([sa.func.REGEXP_MATCHES(text, pattern, "g")]).limit(1).offset(ind - 1).as_scalar(),
    #                 sa_postgresql.ARRAY(sa.String),
    #             )[1],
    #         ),
    #     ]
    # ),
    # regexp_match
    # base.FuncRegexpMatch(
    #     variants=[
    #         V(D.TRINO, lambda text, pattern: text.op("~")(pattern)),
    #     ]
    # ),
    # regexp_replace
    # base.FuncRegexpReplace(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda text, patt, repl: sa.func.REGEXP_REPLACE(text, patt, repl, "g"),
    #         ),
    #     ]
    # ),
    # replace
    # base.FuncReplace.for_dialect(D.TRINO),
    # right
    # base.FuncRight.for_dialect(D.TRINO),
    # rtrim
    # base.FuncRtrim.for_dialect(D.TRINO),
    # space
    # base.FuncSpaceConst.for_dialect(D.TRINO),
    # base.FuncSpaceNonConst(
    #     variants=[
    #         V(D.TRINO, lambda size: sa.func.REPEAT(" ", size)),
    #     ]
    # ),
    # split
    # base.FuncSplit1(
    #     variants=[
    #         V(D.TRINO, lambda text: sa.func.string_to_array(text, ",")),
    #     ]
    # ),
    # base.FuncSplit2(
    #     variants=[
    #         V(D.TRINO, lambda text, delim: sa.func.string_to_array(text, delim)),
    #     ]
    # ),
    # base.FuncSplit3(
    #     variants=[
    #         V(
    #             D.TRINO,
    #             lambda text, delim, ind: sa.func.SPLIT_PART(text, delim, sa.cast(ind, sa.INTEGER)),
    #         ),  # FIXME: does not work with negative indices
    #     ]
    # ),
    # startswith
    # base.FuncStartswithConst.for_dialect(D.TRINO),
    # base.FuncStartswithNonConst(
    #     variants=[
    #         V(D.TRINO, lambda x, y: sa.func.STRPOS(x, y) == 1),
    #     ]
    # ),
    # base.FuncStartswithNonString.for_dialect(D.TRINO),
    # substr
    # base.FuncSubstr2.for_dialect(D.TRINO),
    # base.FuncSubstr3.for_dialect(D.TRINO),
    # trim
    # base.FuncTrim.for_dialect(D.TRINO),
    # upper
    # base.FuncUpperConst.for_dialect(D.TRINO),
    # base.FuncUpperNonConst.for_dialect(D.TRINO),
]
