from typing import (
    Any,
    Sequence,
    TypeVar,
)

import attr

from bi_external_api.domain.utils import ensure_tuple

from .common import DashJSONBuilder


_BUILDER_TV = TypeVar("_BUILDER_TV", bound="DashJSONBuilderSingleTab")


@attr.s(frozen=True)
class DashJSONBuilderSingleTab(DashJSONBuilder):
    chart_names: Sequence[str] = attr.ib(
        default=(),
        converter=ensure_tuple,  # type: ignore
    )

    def add_chart(self: _BUILDER_TV, chart_name: str) -> _BUILDER_TV:
        return attr.evolve(self, chart_names=[*self.chart_names, chart_name])

    def add_charts(self: _BUILDER_TV, chart_name: Sequence[str]) -> _BUILDER_TV:
        return attr.evolve(self, chart_names=[*self.chart_names, *chart_name])

    def build_internal(self) -> dict[str, Any]:
        chart_line_start = 2
        chart_size = 9
        max_width = 36

        selector_items: list[dict[str, Any]] = []

        chart_items: list[dict[str, Any]] = [
            dict(
                id=f"tiid_chart_{idx}",
                element=dict(
                    kind="charts_container",
                    tabs=[
                        dict(
                            id=f"wtid_{chart_name}",
                            chart_name=chart_name,
                            **(dict(title=chart_name) if self.fill_defaults else {}),
                        )
                    ],
                    hide_title=True,
                    **(dict(default_active_chart_tab_id=f"wtid_{chart_name}") if self.fill_defaults else {}),
                ),
                placement=dict(
                    x=(idx * chart_size) % max_width,
                    y=((idx * chart_size) // max_width) + chart_line_start,
                    w=chart_size,
                    h=chart_size,
                ),
            )
            for idx, chart_name in enumerate(self.chart_names)
        ]

        return dict(
            tabs=[
                dict(
                    title="Default",
                    id="default",
                    items=selector_items + chart_items,
                    ignored_connections=[],
                )
            ],
        )
