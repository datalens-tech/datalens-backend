from typing import Any

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from trino.sqlalchemy.compiler import TrinoSQLCompiler


@compiles(TrinoSQLCompiler.FirstValue)
@compiles(TrinoSQLCompiler.LastValue)
@compiles(TrinoSQLCompiler.NthValue)
@compiles(TrinoSQLCompiler.Lead)
@compiles(TrinoSQLCompiler.Lag)
def compile_ignore_nulls(
    element: TrinoSQLCompiler.GenericIgnoreNulls,
    compiler: SQLCompiler,
    **kwargs: Any,
) -> str:
    compiled = f"{element.name}({compiler.process(element.clauses, **kwargs)})"
    if element.ignore_nulls:
        compiled += " IGNORE NULLS"
    return compiled
