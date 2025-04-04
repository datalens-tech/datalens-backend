from typing import Any

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.expression import FunctionElement


class TrinoArray(FunctionElement):
    """Custom array constructor for Trino"""

    name = "trino_array"
    inherit_cache = True


@compiles(TrinoArray)
def _compile_trino_array(element: TrinoArray, compiler: SQLCompiler, **kw: Any) -> str:
    clauses = []
    for clause in element.clauses:
        clauses.append(compiler.process(clause, **kw))

    return f"ARRAY[{', '.join(clauses)}]"
