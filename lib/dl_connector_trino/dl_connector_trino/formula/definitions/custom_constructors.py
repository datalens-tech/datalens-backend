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


class TrinoLambda(FunctionElement):
    """Custom lambda function constructor for Trino"""

    name = "trino_lambda"
    inherit_cache = True


@compiles(TrinoLambda)
def _compile_trino_lambda(element: TrinoLambda, compiler: SQLCompiler, **kw: Any) -> str:
    clauses = list(element.clauses)
    if len(clauses) < 2:
        raise ValueError("TrinoLambda requires at least two arguments: lambda parameters and lambda body")
    *params_exprs, body_expr = clauses
    params_compiled = [compiler.process(expr, **kw) for expr in params_exprs]
    body_compiled = compiler.process(body_expr, **kw)
    params_sql = params_compiled[0] if len(params_compiled) == 1 else f"({', '.join(params_compiled)})"
    return f"{params_sql} -> {body_compiled}"
