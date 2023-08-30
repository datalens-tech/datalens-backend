from __future__ import annotations

from typing import Any, Optional, Tuple, NamedTuple


class NodeHierarchyIndex(NamedTuple):
    indices: Tuple[int, ...] = ()

    @staticmethod
    def make(*args: int) -> NodeHierarchyIndex:
        return NodeHierarchyIndex(indices=args)

    def __bool__(self) -> bool:
        return bool(self.indices)

    def __add__(self, other: Any) -> NodeHierarchyIndex:
        if isinstance(other, int):
            return NodeHierarchyIndex(indices=self.indices + (other, ))
        if isinstance(other, NodeHierarchyIndex):
            return NodeHierarchyIndex(indices=self.indices + other.indices)
        if isinstance(other, (tuple, list)):
            return NodeHierarchyIndex(indices=self.indices + tuple(other))
        raise TypeError(type(other))

    def lsplit(self) -> Tuple[Optional[int], NodeHierarchyIndex]:
        if not self:
            return None, NodeHierarchyIndex()
        return self.indices[0], NodeHierarchyIndex(indices=self.indices[1:])

    def rsplit(self) -> Tuple[NodeHierarchyIndex, Optional[int]]:
        if not self:
            return NodeHierarchyIndex(), None
        return NodeHierarchyIndex(indices=self.indices[:-1]), self.indices[-1]

    def startswith(self, other: NodeHierarchyIndex) -> bool:
        return self.indices[:len(other.indices)] == other.indices
