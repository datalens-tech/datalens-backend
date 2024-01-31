from __future__ import annotations

from typing import (
    Hashable,
    NamedTuple,
    Optional,
    Tuple,
)


class NodeExtract(NamedTuple):
    """
    A hashable syntax-insensitive representation of the node
    that contains all of its essential attributes.
    For formula inspection and optimization purposes.
    """

    type_name: str
    value: Optional[Hashable] = None  # Python primitive for internal non-FormulaItem attributes
    children: Tuple["NodeExtract", ...] = ()
    complexity: int = 1  # A simple and fast way to know the complexity of the expression

    def __repr__(self) -> str:
        return tuple.__repr__(self)

    def __str__(self) -> str:
        return self.__repr__()
