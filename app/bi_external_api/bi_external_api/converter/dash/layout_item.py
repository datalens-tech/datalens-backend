from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import dashboards


class LayoutItemConverter:
    @classmethod
    def convert_ext_to_int(cls, ext_layout_item: ext.DashTabItemPlacement, tab_item_id: str) -> dashboards.LayoutItem:
        return dashboards.LayoutItem(
            h=ext_layout_item.h,
            w=ext_layout_item.w,
            x=ext_layout_item.x,
            y=ext_layout_item.y,
            i=tab_item_id,
        )

    @classmethod
    def convert_int_to_ext(cls, layout_item: dashboards.LayoutItem) -> tuple[ext.DashTabItemPlacement, str]:
        return (
            ext.DashTabItemPlacement(
                h=layout_item.h,
                w=layout_item.w,
                x=layout_item.x,
                y=layout_item.y,
            ),
            layout_item.i,
        )
