from __future__ import annotations

from collections import defaultdict
from itertools import count

import dl_formula.core.nodes as formula_nodes


class NameGen:  # TODO: Rename
    """
    Minor helper for numbered field names.

    >>> ng = NameGen()
    >>> ng(None, 'fld1')
    'fld1_1'
    >>> ng(None, 'fld2')
    'fld2_1'
    >>> ng(None, 'fld1')
    'fld1_2'
    >>> NameGen()(None, 'fld2')
    'fld2_1'
    """

    sep = "_"

    def __init__(self) -> None:
        self.counters = defaultdict(lambda: 0)  # type: ignore  # 2024-01-24 # TODO: Need type annotation for "counters"  [var-annotated]

    def __call__(self, node: formula_nodes.FormulaItem, name: str) -> str:
        self.counters[name] += 1
        return f"{name}{self.sep}{self.counters[name]}"


class MappedNameGen(NameGen):
    sep = ""

    def __init__(self, namemap) -> None:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        super().__init__()
        self.namemap = namemap

    def __call__(self, node, name) -> str:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        return super().__call__(node, self.namemap.get(name, name))


class PrefixedIdGen:
    """
    Simplistic non-random prefixed ID generator.
    """

    def __init__(self, prefix: str) -> None:
        self._prefix = prefix
        self._cnt = count()

    def get_id(self) -> str:
        return f"{self._prefix}_{next(self._cnt)}"
