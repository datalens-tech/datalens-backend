from abc import abstractmethod
from collections.abc import Sequence
from typing import (
    Any,
    Union,
)

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.elements import (
    ClauseElement,
    ClauseList,
)
from sqlalchemy.sql.expression import (
    FunctionElement,
    literal,
)


class BaseTrinoArray(FunctionElement):
    """Base class for Trino array constructors"""

    name: str
    inherit_cache: bool = True

    def __init__(self, *elements: Any) -> None:
        super().__init__()
        self.clauses = ClauseList(*self._process_elements(elements))

    @abstractmethod
    def _process_elements(self, elements: Sequence[Any]) -> list[ClauseElement]:
        raise NotImplementedError


@compiles(BaseTrinoArray)
def _compile_trino_array(element: BaseTrinoArray, compiler: SQLCompiler, **kw: Any) -> str:
    clauses: list[str] = [compiler.process(c, **kw) for c in element.clauses]  # type: ignore[attr-defined]
    return f"ARRAY[{', '.join(clauses)}]"


class TrinoNonConstArray(BaseTrinoArray):
    """Array constructor for SQL expressions/columns"""

    name: str = "trino_non_const_array"

    def _process_elements(self, elements: Sequence[ClauseElement]) -> list[ClauseElement]:
        return list(elements)


class TrinoConstArray(BaseTrinoArray):
    """Array constructor for literal values"""

    name: str = "trino_const_array"

    def _process_elements(self, elements: Sequence[Union[int, float, str, bool, None]]) -> list[ClauseElement]:
        return [literal(e) for e in elements]
