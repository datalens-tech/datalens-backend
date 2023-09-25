from __future__ import annotations

import abc
from itertools import chain
from typing import (
    AbstractSet,
    Callable,
    Iterable,
)

import attr

from dl_api_client.dsmaker.data_abstraction.primitives import (
    DataCellTuple,
    DataItem,
    DataItemTag,
)


class DataCellMapper1D(abc.ABC):
    """
    Mapping-like interface for data where a single `DataCellTuple`
    defines a single measure cell.
    """

    @abc.abstractmethod
    def items(self) -> Iterable[tuple[DataCellTuple, DataItem]]:
        raise NotImplementedError

    def as_dict(self) -> dict[DataCellTuple, DataItem]:
        return {key: value for key, value in self.items()}

    @abc.abstractmethod
    def get_value_count(self) -> int:
        raise NotImplementedError

    def apply_tagger(
        self,
        tagger: Callable[[DataCellTuple, DataItem], AbstractSet[DataItemTag]],
    ) -> DataCellMapper1D:
        return TaggerProxyDataCellMapper1D(nested=self, tagger=tagger)

    def apply_filter(
        self,
        require_tag_combos: AbstractSet[frozenset[DataItemTag]] = frozenset(),
        exclude_tag_combos: AbstractSet[frozenset[DataItemTag]] = frozenset(),
    ) -> DataCellMapper1D:
        return FilterProxyDataCellMapper1D(
            nested=self,
            require_tag_combos=require_tag_combos,
            exclude_tag_combos=exclude_tag_combos,
        )


@attr.s(frozen=True)
class SimpleDataCellMapper1D(DataCellMapper1D):
    """
    A simple dict-based implementation of `DataCellMapper1D`
    """

    _cells: dict[DataCellTuple, DataItem] = attr.ib()

    def items(self) -> Iterable[tuple[DataCellTuple, DataItem]]:
        return ((key, value) for key, value in self._cells.items())

    def as_dict(self) -> dict[DataCellTuple, DataItem]:
        return self._cells

    def get_value_count(self) -> int:
        return len(self._cells)


@attr.s(frozen=True)
class TaggerProxyDataCellMapper1D(DataCellMapper1D):
    """
    A proxy mapper that adds tags to items of the nested mapper.
    """

    nested: DataCellMapper1D = attr.ib(kw_only=True)
    tagger: Callable[[DataCellTuple, DataItem], AbstractSet[DataItemTag]] = attr.ib(kw_only=True)

    def items(self) -> Iterable[tuple[DataCellTuple, DataItem]]:
        for dims, item in self.nested.items():
            new_tags = self.tagger(dims, item)
            if new_tags != item.meta.tags:
                item = item.clone(meta=item.meta.clone(tags=frozenset(new_tags)))
            item = item
            yield dims, item

    def get_value_count(self) -> int:
        return self.nested.get_value_count()


@attr.s(frozen=True)
class FilterProxyDataCellMapper1D(DataCellMapper1D):
    """
    A proxy mapper that filters the contents of a nested mapper.
    """

    nested: DataCellMapper1D = attr.ib(kw_only=True)
    require_tag_combos: AbstractSet[frozenset[DataItemTag]] = attr.ib(kw_only=True, factory=frozenset)
    exclude_tag_combos: AbstractSet[frozenset[DataItemTag]] = attr.ib(kw_only=True, factory=frozenset)

    def __attrs_post_init__(self) -> None:
        assert not self.require_tag_combos and self.exclude_tag_combos, "Cannot specify both requirements and excludes"

    def items(self) -> Iterable[tuple[DataCellTuple, DataItem]]:
        for dims, item in self.nested.items():
            # if there are requirements, apply them
            if self.require_tag_combos and not any(
                item.meta.tags.issuperset(combo) for combo in self.require_tag_combos
            ):
                continue

            # if there are excludes, apply them
            if any(item.meta.tags.issuperset(combo) for combo in self.exclude_tag_combos):
                continue

            yield dims, item

    def get_value_count(self) -> int:
        return len(list(self.items()))


class DataCellMapper2D(abc.ABC):
    """
    Mapping-like interface for data where a pair of `DataCellTuple` instances
    defines a single measure cell.
    Can be converted to a `DataCellMapper1D` via the `as_1d_mapper` method.
    """

    @abc.abstractmethod
    def items(self) -> Iterable[tuple[tuple[DataCellTuple, DataCellTuple], DataItem]]:
        raise NotImplementedError

    def as_dict(self) -> dict[tuple[DataCellTuple, DataCellTuple], DataItem]:
        return {key: value for key, value in self.items()}

    def _make_1d_coords(self, orig_dim_coords: tuple[DataCellTuple, DataCellTuple]) -> DataCellTuple:
        """
        Turn a pair of `DataCellTuple`s into a single `DataCellTuple`.
        Sort the data cells by cell title.
        """
        return DataCellTuple(
            tuple(
                sorted(
                    (dim_cell for dim_cell in chain(orig_dim_coords[0].cells, orig_dim_coords[1].cells)),
                    key=lambda _cell: _cell.title,
                )
            )
        )

    def as_1d_mapper(self) -> DataCellMapper1D:
        """
        Convert into a `DataCellMapper1D` by flattening the dimension coord pair into a single `DataCellTuple`
        """

        cells_1d = {self._make_1d_coords(orig_dim_coords): value for orig_dim_coords, value in self.items()}
        return SimpleDataCellMapper1D(cells_1d)
