import sqlalchemy as sa

import dl_formula.definitions.functions_string as base
from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import make_binary_chain

from bi_connector_oracle.formula.constants import OracleDialect as D


V = TranslationVariant.make


DEFINITIONS_STRING = [
    # ascii
    base.FuncAscii.for_dialect(D.ORACLE),

    # char
    base.FuncChar(variants=[
        V(D.ORACLE, sa.func.CHR),
    ]),

    # concat
    base.Concat1.for_dialect((D.ORACLE)),
    base.ConcatMultiStrConst.for_dialect(D.ORACLE),
    base.ConcatMultiStr(variants=[
        V(D.ORACLE, lambda *args: make_binary_chain(
            # chain of 2-arg CONCAT function calls
            (lambda x, y: sa.func.CONCAT(x, y)), *args, wrap_as_nodes=False
        )),
    ]),
    base.ConcatMultiAny.for_dialect(D.ORACLE),

    # contains
    base.FuncContainsConst.for_dialect(D.ORACLE),
    base.FuncContainsNonConst(variants=[
        V(D.ORACLE, lambda x, y: sa.func.INSTR(x, y) > 0),
    ]),
    base.FuncContainsNonString.for_dialect(D.ORACLE),

    # endswith
    base.FuncEndswithConst.for_dialect(D.ORACLE),
    base.FuncEndswithNonConst(variants=[
        V(D.ORACLE, lambda x, y: (
            sa.func.SUBSTR(x, sa.func.LENGTH(x) - sa.func.LENGTH(y) + 1, sa.func.LENGTH(y)) == y)),
    ]),
    base.FuncEndswithNonString.for_dialect(D.ORACLE),

    # find
    base.FuncFind2(variants=[
        V(D.ORACLE, sa.func.INSTR),
    ]),
    base.FuncFind3(variants=[
        V(D.ORACLE, sa.func.INSTR),
    ]),

    # icontains
    base.FuncIContainsNonConst.for_dialect(D.ORACLE),
    base.FuncIContainsNonString.for_dialect(D.ORACLE),

    # iendswith
    base.FuncIEndswithNonConst.for_dialect(D.ORACLE),
    base.FuncIEndswithNonString.for_dialect(D.ORACLE),

    # istartswith
    base.FuncIStartswithNonConst.for_dialect(D.ORACLE),
    base.FuncIStartswithNonString.for_dialect(D.ORACLE),

    # left
    base.FuncLeft(variants=[
        V(D.ORACLE, lambda x, y: sa.func.SUBSTR(x, 1, y)),
    ]),

    # len
    base.FuncLenString(variants=[
        V(D.ORACLE, sa.func.LENGTH),
    ]),

    # lower
    base.FuncLowerConst.for_dialect(D.ORACLE),
    base.FuncLowerNonConst.for_dialect(D.ORACLE),

    # ltrim
    base.FuncLtrim.for_dialect(D.ORACLE),

    # regexp_extract
    base.FuncRegexpExtract(variants=[
        V(D.ORACLE, sa.func.REGEXP_SUBSTR),
    ]),

    # regexp_extract_nth
    base.FuncRegexpExtractNth(variants=[
        V(D.ORACLE, lambda text, pattern, ind: sa.func.REGEXP_SUBSTR(text, pattern, 1, ind)),
    ]),

    # regexp_match
    base.FuncRegexpMatch(variants=[
        V(D.ORACLE, lambda text, pattern: sa.func.REGEXP_SUBSTR(text, pattern).isnot(None)),
    ]),

    # regexp_replace
    base.FuncRegexpReplace(variants=[
        V(D.ORACLE, sa.func.REGEXP_REPLACE),
    ]),

    # replace
    base.FuncReplace.for_dialect(D.ORACLE),

    # right
    base.FuncRight(variants=[
        V(D.ORACLE, lambda x, y: sa.func.SUBSTR(x, sa.func.LENGTH(x) - y + 1, y)),
    ]),

    # rtrim
    base.FuncRtrim.for_dialect(D.ORACLE),

    # space
    base.FuncSpaceConst.for_dialect(D.ORACLE),
    base.FuncSpaceNonConst(variants=[
        V(D.ORACLE, lambda size: sa.func.SUBSTR(sa.func.LPAD('.', size + 1, ' '), 1, size)),
    ]),

    # startswith
    base.FuncStartswithConst.for_dialect(D.ORACLE),
    base.FuncStartswithNonConst(variants=[
        V(D.ORACLE, lambda x, y: (sa.func.SUBSTR(x, 1, sa.func.LENGTH(y)) == y)),
    ]),
    base.FuncStartswithNonString.for_dialect(D.ORACLE),

    # substr
    base.FuncSubstr2(variants=[
        V(D.ORACLE, lambda text, start: sa.func.SUBSTR(text, start, sa.func.LENGTH(text) - start + 1)),
    ]),
    base.FuncSubstr3(variants=[
        V(D.ORACLE, sa.func.SUBSTR),
    ]),

    # trim
    base.FuncTrim.for_dialect(D.ORACLE),

    # upper
    base.FuncUpperConst.for_dialect(D.ORACLE),
    base.FuncUpperNonConst.for_dialect(D.ORACLE),
]
