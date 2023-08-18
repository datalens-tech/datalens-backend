from __future__ import annotations

from typing import Any, FrozenSet, NamedTuple


class LevelTag(NamedTuple):
    bfb_names: FrozenSet
    func_nesting: int
    qfork_nesting: int

    def __gt__(self, other: Any) -> bool:
        if type(other) is not type(self):
            raise TypeError(type(other))
        return (
            (self.bfb_names != other.bfb_names and self.bfb_names.issuperset(other.bfb_names))
            or (self.bfb_names == other.bfb_names and self.func_nesting > other.func_nesting)
            or (self.bfb_names == other.bfb_names and self.func_nesting == other.func_nesting
                and self.qfork_nesting > other.qfork_nesting)
        )

    def __ge__(self, other: Any) -> bool:
        if type(other) is not type(self):
            raise TypeError(type(other))
        return (
            (self.bfb_names != other.bfb_names and self.bfb_names.issuperset(other.bfb_names))
            or (self.bfb_names == other.bfb_names and self.func_nesting > other.func_nesting)
            or (self.bfb_names == other.bfb_names and self.func_nesting == other.func_nesting
                and self.qfork_nesting >= other.qfork_nesting)
        )

    def __lt__(self, other: Any) -> bool:
        if type(other) is not type(self):
            raise TypeError(type(other))
        return (
            (self.bfb_names != other.bfb_names and self.bfb_names.issubset(other.bfb_names))
            or (self.bfb_names == other.bfb_names and self.func_nesting < other.func_nesting)
            or (self.bfb_names == other.bfb_names and self.func_nesting == other.func_nesting
                and self.qfork_nesting < other.qfork_nesting)
        )

    def __le__(self, other: Any) -> bool:
        if type(other) is not type(self):
            raise TypeError(type(other))
        return (
            (self.bfb_names != other.bfb_names and self.bfb_names.issubset(other.bfb_names))
            or (self.bfb_names == other.bfb_names and self.func_nesting < other.func_nesting)
            or (self.bfb_names == other.bfb_names and self.func_nesting == other.func_nesting
                and self.qfork_nesting <= other.qfork_nesting)
        )

    # use default eq & ne
