from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler

import dl_sqlalchemy_ydb.dialect as ydb_dialect


@compiles(ydb_dialect.YqlListLiteral)
def _compile_list_literal(element: ydb_dialect.YqlListLiteral, compiler: SQLCompiler, **kw: Any) -> str:
    return compiler.process(sa.func.AsList(*element.clauses), **kw)
