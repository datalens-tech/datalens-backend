from typing import Any

import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_string as base

from dl_connector_mssql.formula.constants import MssqlDialect as D


V = TranslationVariant.make


def make_like_pattern_mssql_const(value: Any, left_any: bool = True, right_any: bool = True) -> str:
    assert isinstance(value, str)
    # XXXX: does not handle `[` / `]` in the value.
    # Probably should be
    # `value = re.sub(r'([\[%])', r'[\1]', value)`
    # https://stackoverflow.com/q/439495
    value = value.replace("%", "[%]")
    result = "{}{}{}".format(
        "%" if left_any else "",
        value,
        "%" if right_any else "",
    )
    # result = literal(result)
    return result


def make_like_pattern_mssql_clause(clause, left_any=True, right_any=True):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
    # e.g. `ColumnClause`
    pieces = []
    if left_any:
        pieces += ["%"]
    # XXXX: need at least the `REPLACE(…, '%', '[%]')` wrapping here.
    pieces += [clause]
    if right_any:
        pieces += ["%"]
    return sa.func.CONCAT(*pieces)


DEFINITIONS_STRING = [
    # ascii
    base.FuncAscii.for_dialect(D.MSSQLSRV),
    # char
    base.FuncChar.for_dialect(D.MSSQLSRV),
    # concat
    base.Concat1.for_dialect((D.MSSQLSRV)),
    base.ConcatMultiStrConst.for_dialect(D.MSSQLSRV),
    base.ConcatMultiStr.for_dialect(D.MSSQLSRV),
    base.ConcatMultiAny.for_dialect(D.MSSQLSRV),
    # contains
    base.FuncContainsConst(
        variants=[
            V(
                D.MSSQLSRV,
                lambda x, y: x.like(
                    make_like_pattern_mssql_const(y.value, left_any=True, right_any=True),
                ),
            ),
        ]
    ),
    base.FuncContainsNonConst(
        variants=[
            V(
                D.MSSQLSRV,
                lambda x, y: sa.func.PATINDEX(make_like_pattern_mssql_clause(y, left_any=True, right_any=True), x) > 0,
            ),
        ]
    ),
    base.FuncContainsNonString.for_dialect(D.MSSQLSRV),
    # notcontains
    base.FuncNotContainsConst.for_dialect(D.MSSQLSRV),
    base.FuncNotContainsNonConst.for_dialect(D.MSSQLSRV),
    base.FuncNotContainsNonString.for_dialect(D.MSSQLSRV),
    # endswith
    base.FuncEndswithConst(
        variants=[
            V(
                D.MSSQLSRV,
                lambda x, y: x.like(
                    make_like_pattern_mssql_const(y.value, left_any=True, right_any=False),
                ),
            ),
        ]
    ),
    base.FuncEndswithNonConst(
        variants=[
            V(
                D.MSSQLSRV,
                lambda x, y: (sa.func.SUBSTRING(x, sa.func.LEN(x) - sa.func.LEN(y) + 1, sa.func.LEN(x)) == y),
            ),
        ]
    ),
    base.FuncEndswithNonString.for_dialect(D.MSSQLSRV),
    # find
    base.FuncFind2(
        variants=[
            V(
                D.MSSQLSRV,
                lambda x, y: sa.func.PATINDEX(make_like_pattern_mssql_clause(y, left_any=True, right_any=True), x),
            ),
        ]
    ),
    base.FuncFind3(
        variants=[
            V(
                D.MSSQLSRV,
                lambda x, y, z: sa.func.IIF(
                    sa.func.PATINDEX(
                        make_like_pattern_mssql_clause(y, left_any=True, right_any=True),
                        sa.func.SUBSTRING(x, z, sa.func.DATALENGTH(x)),
                    )
                    > 0,
                    sa.func.PATINDEX(
                        make_like_pattern_mssql_clause(y, left_any=True, right_any=True),
                        sa.func.SUBSTRING(x, z, sa.func.DATALENGTH(x)),
                    )
                    + z
                    - 1,
                    0,
                ),
            ),
        ]
    ),
    # icontains
    base.FuncIContainsNonConst.for_dialect(D.MSSQLSRV),
    base.FuncIContainsNonString.for_dialect(D.MSSQLSRV),
    # iendswith
    base.FuncIEndswithNonConst.for_dialect(D.MSSQLSRV),
    base.FuncIEndswithNonString.for_dialect(D.MSSQLSRV),
    # istartswith
    base.FuncIStartswithNonConst.for_dialect(D.MSSQLSRV),
    base.FuncIStartswithNonString.for_dialect(D.MSSQLSRV),
    # left
    base.FuncLeft.for_dialect(D.MSSQLSRV),
    # len
    base.FuncLenString(
        variants=[
            V(D.MSSQLSRV, sa.func.LEN),
        ]
    ),
    # lower
    base.FuncLowerConst.for_dialect(D.MSSQLSRV),
    base.FuncLowerNonConst.for_dialect(D.MSSQLSRV),
    # ltrim
    base.FuncLtrim.for_dialect(D.MSSQLSRV),
    # replace
    base.FuncReplace.for_dialect(D.MSSQLSRV),
    # right
    base.FuncRight.for_dialect(D.MSSQLSRV),
    # rtrim
    base.FuncRtrim.for_dialect(D.MSSQLSRV),
    # space
    base.FuncSpaceConst.for_dialect(D.MSSQLSRV),
    base.FuncSpaceNonConst.for_dialect(D.MSSQLSRV),
    # startswith
    base.FuncStartswithConst(
        variants=[
            V(
                D.MSSQLSRV,
                lambda x, y: x.like(
                    make_like_pattern_mssql_const(y.value, left_any=False, right_any=True),
                ),
            ),
        ]
    ),
    base.FuncStartswithNonConst(
        variants=[
            V(D.MSSQLSRV, lambda x, y: sa.func.SUBSTRING(x, 1, sa.func.LEN(y)) == y),
        ]
    ),
    base.FuncStartswithNonString.for_dialect(D.MSSQLSRV),
    # substr
    base.FuncSubstr2(
        variants=[
            V(D.MSSQLSRV, lambda text, start: sa.func.SUBSTRING(text, start, sa.func.DATALENGTH(text))),
        ]
    ),
    base.FuncSubstr3.for_dialect(D.MSSQLSRV),
    # trim
    base.FuncTrim.for_dialect(D.MSSQLSRV),
    # upper
    base.FuncUpperConst.for_dialect(D.MSSQLSRV),
    base.FuncUpperNonConst.for_dialect(D.MSSQLSRV),
]
