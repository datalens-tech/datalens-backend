from typing import Optional

import attr

from bi_external_api.converter.converter_exc import MalformedEntryConfig
from bi_external_api.domain.internal import charts


@attr.s(frozen=True)
class InternalVisualizationAccessor:
    vis: charts.Visualization = attr.ib()

    def get_single_placeholder_or_none(self, plh_id: charts.PlaceholderId) -> Optional[charts.Placeholder]:
        filtered_placeholder_list = [
            ph
            for ph in self.vis.placeholders
            if ph.id == plh_id
        ]
        if len(filtered_placeholder_list) == 0:
            return None
        if len(filtered_placeholder_list) != 1:
            raise MalformedEntryConfig(
                f"Chart has {len(filtered_placeholder_list)} > 1 placeholder of type {plh_id.value}"
            )

        return filtered_placeholder_list[0]

    def get_single_placeholder(self, plh_id: charts.PlaceholderId) -> charts.Placeholder:
        may_be_placeholder = self.get_single_placeholder_or_none(plh_id)
        if may_be_placeholder is None:
            raise MalformedEntryConfig(f"Chart has no placeholder {plh_id}")
        return may_be_placeholder

    def get_single_placeholder_item(self, plh_id: charts.PlaceholderId) -> charts.ChartField:
        placeholder = self.get_single_placeholder(plh_id)
        all_items = placeholder.items

        if len(all_items) == 1:
            return all_items[0]

        raise MalformedEntryConfig(f"Chart placeholder {plh_id.value} has {len(all_items)} != 1 items")
