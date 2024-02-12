from __future__ import annotations

from typing import (
    Any,
    Dict,
    Generic,
    Iterable,
    Optional,
    Set,
    TypeVar,
)

import attr

import dl_formula.core.exc as exc
from dl_formula.core.extract import NodeExtract
import dl_formula.core.nodes as nodes


def validate_node_is_extractable(node: nodes.FormulaItem) -> nodes.NodeExtract:
    if node.extract is None:
        raise exc.CacheError("Node has no extract")
    return node.extract


class NodeSet:
    __slots__ = ("_data_set",)

    def __init__(self, nodes: Iterable[nodes.FormulaItem] = ()):
        self._data_set: Set[NodeExtract] = set()
        for node in nodes:
            self.add(node)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self._data_set == other._data_set

    def add(self, node: nodes.FormulaItem) -> None:
        extract = validate_node_is_extractable(node)
        self._data_set.add(extract)

    def remove(self, node: nodes.FormulaItem) -> None:
        if node.extract is None:
            return

        self._data_set.remove(node.extract)

    def __contains__(self, node: nodes.FormulaItem) -> bool:
        return node.extract in self._data_set

    def clear(self) -> None:
        self._data_set.clear()

    def __or__(self, other: NodeSet) -> NodeSet:
        result = NodeSet()
        result._data_set = self._data_set | other._data_set
        return result

    def __and__(self, other: NodeSet) -> NodeSet:
        result = NodeSet()
        result._data_set = self._data_set & other._data_set
        return result

    def __len__(self) -> int:
        return len(self._data_set)


_MAP_VALUE_TV = TypeVar("_MAP_VALUE_TV")


@attr.s
class NodeValueMap(Generic[_MAP_VALUE_TV]):
    _data: Dict[NodeExtract, _MAP_VALUE_TV] = attr.ib(factory=dict)

    def add(self, node: nodes.FormulaItem, value: _MAP_VALUE_TV) -> None:
        extract = validate_node_is_extractable(node)
        self._data[extract] = value

    def get(self, node: nodes.FormulaItem) -> Optional[_MAP_VALUE_TV]:
        if node.extract is None:
            return None
        return self._data.get(node.extract)

    def __contains__(self, node: nodes.FormulaItem) -> bool:
        return node.extract in self._data

    def clear(self) -> None:
        self._data.clear()

    def __len__(self) -> int:
        return len(self._data)
