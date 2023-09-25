from typing import Iterator

import attr

from dl_api_client.dsmaker.primitives import LegendItem


@attr.s(frozen=True)
class FieldLegend:
    fields: tuple[LegendItem, ...] = attr.ib(converter=tuple)
    _liid_to_item_map: dict[int, LegendItem] = attr.ib(init=False)

    @_liid_to_item_map.default
    def _make_liid_to_item_map(self) -> dict[int, LegendItem]:
        return {field.legend_item_id: field for field in self.fields}

    def get_item_by_legend_item_id(self, legend_item_id: int) -> LegendItem:
        return self._liid_to_item_map[legend_item_id]

    def get_title_by_legend_item_id(self, legend_item_id: int) -> str:
        return self._liid_to_item_map[legend_item_id].title

    def __iter__(self) -> Iterator[LegendItem]:
        return iter(self.fields)
