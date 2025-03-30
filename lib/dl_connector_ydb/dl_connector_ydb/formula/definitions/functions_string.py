import sqlalchemy as sa
import ydb_sqlalchemy.sqlalchemy as ydb_sa

from dl_formula.definitions.base import TranslationVariant
from dl_formula.definitions.common import (
    ifnotnull,
    make_binary_chain,
)
import dl_formula.definitions.functions_string as base

from dl_connector_ydb.formula.constants import YqlDialect as D


V = TranslationVariant.make


DEFINITIONS_STRING = [
    # ascii
    base.FuncAscii(
        variants=[
            # ListHead(Unicode::ToCodePointList(Unicode::Substring('ы', 0, 1))) = 1099
            V(
                D.YQL,
                lambda x: sa.func.ListHead(
                    sa.func.Unicode.ToCodePointList(
                        sa.func.Unicode.Substring(
                            sa.cast(x, sa.TEXT), sa.cast(0, ydb_sa.types.UInt64), sa.cast(1, ydb_sa.types.UInt64)
                        )
                    )
                ),
            ),
        ]
    ),
    # char
    base.FuncChar(
        variants=[
            # CASE WHEN value IS NULL THEN NULL ELSE
            #     Unicode::FromCodePointList(AsList(COALESCE(CAST(value AS UInt32), 0)))
            # END
            V(
                D.YQL,
                lambda value: ifnotnull(
                    value,
                    # int -> List<int> -> utf8
                    sa.func.Unicode.FromCodePointList(
                        sa.func.AsList(
                            # coalesce is needed to un-Nullable the type.
                            sa.func.COALESCE(sa.cast(value, ydb_sa.types.UInt32), sa.cast(0, ydb_sa.types.UInt32)),
                        )
                    ),
                ),
            ),
        ]
    ),
    # concat
    base.Concat1.for_dialect((D.YQL)),
    base.ConcatMultiStrConst.for_dialect(D.YQL),
    base.ConcatMultiStr(
        variants=[
            V(
                D.YQL,
                lambda *args: make_binary_chain(
                    (lambda x, y: x.concat(y)),  # should result in `x || y` SQL.
                    *args,  # should result in `x || y || z || ...` SQL
                    wrap_as_nodes=False,
                ),
            ),
        ]
    ),
    base.ConcatMultiAny.for_dialect(D.YQL),
    # contains
    base.FuncContainsConst(
        variants=[
            # V(D.YQL,
            #   # # “'%', '_' and '\' are currently not supported in ESCAPE clause,”
            #   lambda x, y: x.like('%{}%'.format(quote_like(y.value, escape='!')), escape='!')),
            # # Allows UTF8; also, notably, does not allow a nullable second argument:
            V(D.YQL, sa.func.String.Contains),
        ]
    ),
    base.FuncContainsNonConst(
        variants=[
            # `''` shouldn't be ever used due to `ifnotnull`.
            V(D.YQL, lambda x, y: ifnotnull(y, sa.func.String.Contains(x, sa.func.COALESCE(y, "")))),
        ]
    ),
    base.FuncContainsNonString.for_dialect(D.YQL),
    # notcontains
    base.FuncNotContainsConst.for_dialect(D.YQL),
    base.FuncNotContainsNonConst.for_dialect(D.YQL),
    base.FuncNotContainsNonString.for_dialect(D.YQL),
    # endswith
    base.FuncEndswithConst(
        variants=[
            V(D.YQL, sa.func.String.EndsWith),
        ]
    ),
    base.FuncEndswithNonConst(
        variants=[
            # `''` shouldn't ever happen due to `ifnotnull`.
            V(D.YQL, lambda x, y: ifnotnull(y, sa.func.String.EndsWith(x, sa.func.COALESCE(y, "")))),
        ]
    ),
    base.FuncEndswithNonString.for_dialect(D.YQL),
    # find
    base.FuncFind2(
        variants=[
            # In YQL indices start from 0, but we count them from 1, so have to do -1/+1 here
            V(D.YQL, lambda text, piece: sa.func.COALESCE(sa.func.Unicode.Find(text, piece), -1) + 1),
        ]
    ),
    base.FuncFind3(
        variants=[
            # In YQL indices start from 0, but we count them from 1, so have to do -1/+1 here
            V(
                D.YQL,
                lambda text, piece, startpos: sa.func.COALESCE(
                    sa.func.Unicode.Find(text, piece, sa.cast(startpos, ydb_sa.types.UInt64)), -1
                )
                + 1,
            ),
        ]
    ),
    # icontains
    base.FuncIContainsNonConst.for_dialect(D.YQL),
    base.FuncIContainsNonString.for_dialect(D.YQL),
    # iendswith
    base.FuncIEndswithNonConst.for_dialect(D.YQL),
    base.FuncIEndswithNonString.for_dialect(D.YQL),
    # istartswith
    base.FuncIStartswithNonConst.for_dialect(D.YQL),
    base.FuncIStartswithNonString.for_dialect(D.YQL),
    # left
    base.FuncLeft(
        variants=[
            V(
                D.YQL,
                lambda x, y: sa.func.Unicode.Substring(
                    sa.cast(x, sa.TEXT), sa.cast(0, ydb_sa.types.UInt64), sa.cast(y, ydb_sa.types.UInt64)
                ),
            ),
        ]
    ),
    # len
    base.FuncLenString(
        variants=[
            V(D.YQL, lambda val: sa.func.Unicode.GetLength(sa.cast(val, sa.TEXT))),
        ]
    ),
    # lower
    base.FuncLowerConst.for_dialect(D.YQL),
    base.FuncLowerNonConst(
        variants=[
            V(D.YQL, lambda val: sa.func.Unicode.ToLower(sa.cast(val, sa.TEXT))),
        ]
    ),
    # regexp_extract
    # TODO: YQL
    # https://ydb.tech/en/docs/yql/reference/udf/list/hyperscan
    # Problem: "By default, all functions work in the single-byte mode.
    # However, if the regular expression is a valid UTF-8 string but is not a valid ASCII string,
    # the UTF-8 mode is enabled automatically." However, we can't use higher-order functions yet.
    # replace
    base.FuncReplace(
        variants=[
            V(
                D.YQL,
                lambda val, repl_from, repl_with: sa.func.Unicode.ReplaceAll(
                    sa.cast(val, sa.TEXT),
                    sa.func.COALESCE(sa.cast(repl_from, sa.TEXT), ""),
                    sa.func.COALESCE(sa.cast(repl_with, sa.TEXT), ""),
                ),
            ),
        ]
    ),
    # right
    base.FuncRight(
        variants=[
            V(
                D.YQL,
                lambda x, y: sa.func.Unicode.Substring(
                    sa.cast(x, sa.TEXT),
                    sa.cast(sa.func.Unicode.GetLength(sa.cast(x, sa.TEXT)) - y, ydb_sa.types.UInt64),
                ),
            ),
        ]
    ),
    # space
    base.FuncSpaceConst.for_dialect(D.YQL),
    base.FuncSpaceNonConst(
        variants=[
            # YQL string multiplication: also consider
            #     sa.func.ListConcat(sa.func.ListReplicate(sa.cast(' ', sa.TEXT), size))
            V(D.YQL, lambda size: sa.cast(sa.func.String.LeftPad("", size, " "), sa.TEXT)),
        ]
    ),
    # split
    base.FuncSplit3(
        variants=[
            V(
                D.YQL,
                lambda text, delim, ind: sa.func.ListHead(
                    sa.func.ListSkip(
                        sa.func.Unicode.SplitToList(sa.cast(text, sa.TEXT), delim),  # must be non-nullable
                        sa.func.UNWRAP(sa.cast(ind - 1, ydb_sa.types.UInt32)),
                    )
                ),
            ),
        ]
    ),
    # startswith
    base.FuncStartswithConst(
        variants=[
            V(D.YQL, sa.func.String.StartsWith),
        ]
    ),
    base.FuncStartswithNonConst(
        variants=[
            # `''` shouldn't ever happen due to `ifnotnull`.
            V(D.YQL, lambda x, y: ifnotnull(y, sa.func.String.StartsWith(x, sa.func.COALESCE(y, "")))),
        ]
    ),
    base.FuncStartswithNonString.for_dialect(D.YQL),
    # substr
    base.FuncSubstr2(
        variants=[
            # In YQL indices start from 0, but we count them from 1, so have to do -1 here
            V(
                D.YQL,
                lambda val, start: sa.func.Unicode.Substring(
                    sa.cast(val, sa.TEXT), sa.func.UNWRAP(sa.cast(start - 1, ydb_sa.types.UInt64))
                ),
            ),
        ]
    ),
    base.FuncSubstr3(
        variants=[
            # In YQL indices start from 0, but we count them from 1, so have to do -1 here
            V(
                D.YQL,
                lambda val, start, length: sa.func.Unicode.Substring(
                    sa.cast(val, sa.TEXT),
                    sa.func.UNWRAP(sa.cast(start - 1, ydb_sa.types.UInt64)),
                    sa.func.UNWRAP(sa.cast(length, ydb_sa.types.UInt64)),
                ),
            ),
        ]
    ),
    # upper
    base.FuncUpperConst.for_dialect(D.YQL),
    base.FuncUpperNonConst(
        variants=[
            V(D.YQL, lambda val: sa.func.Unicode.ToUpper(sa.cast(val, sa.TEXT))),
        ]
    ),
]
