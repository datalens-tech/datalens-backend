from __future__ import annotations

from enum import Enum
from typing import Any

import attr


@attr.s(frozen=True)
class DataCell:
    value: Any = attr.ib(kw_only=True)
    title: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class DataCellTuple:
    cells: tuple[DataCell, ...] = attr.ib()

    @property
    def titles(self) -> frozenset[str]:
        return frozenset(cell.title for cell in self.cells)


class DataItemTag(Enum):
    total = 'total'
    annotation = 'annotation'


@attr.s(frozen=True)
class DataItemMeta:
    tags: frozenset[DataItemTag] = attr.ib(kw_only=True, factory=frozenset)

    def clone(self, **kwargs: Any) -> DataItemMeta:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True)
class DataItem:
    cell: DataCell = attr.ib(kw_only=True)
    meta: DataItemMeta = attr.ib(kw_only=True, factory=DataItemMeta)

    def clone(self, **kwargs: Any) -> DataItem:
        return attr.evolve(self, **kwargs)
