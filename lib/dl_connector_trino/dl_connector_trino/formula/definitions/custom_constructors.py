from typing import (
    Any,
    Union,
)

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.elements import (
    literal,
    literal_column,
)
from sqlalchemy.sql.expression import FunctionElement

from dl_formula.connectors.base.literal import Literal


def trino_array_literal(values: Union[tuple, list]) -> Literal:
    """Create a literal array for Trino."""
    compiled_items = [str(literal(v).compile(compile_kwargs={"literal_binds": True})) for v in values]
    sql = f"ARRAY[{', '.join(compiled_items)}]"
    return literal_column(sql)


class TrinoArray(FunctionElement):
    """Custom array constructor for Trino"""

    name = "trino_array"
    inherit_cache = True


@compiles(TrinoArray)
def _compile_trino_array(element: TrinoArray, compiler: SQLCompiler, **kw: Any) -> str:
    # Handle both column references and literal values
    clauses = []
    for clause in element.clauses:
        # Process each clause with the compiler
        clauses.append(compiler.process(clause, **kw))

    return f"ARRAY[{', '.join(clauses)}]"
