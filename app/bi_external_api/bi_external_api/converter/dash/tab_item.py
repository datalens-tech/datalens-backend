import attr

from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import dashboards
from .dash_element import DashElementConverterFacade
from .layout_item import LayoutItemConverter


@attr.s(frozen=True, auto_attribs=True)
class IntTabItemLayoutItemPair:
    tab_item: dashboards.TabItem
    layout_item: dashboards.LayoutItem


@attr.s(frozen=True, auto_attribs=True)
class BaseDashboardItemConverter:
    _wb_context: WorkbookContext = attr.ib()

    def create_tab_item_dash_element_converter_facade(self) -> DashElementConverterFacade:
        return DashElementConverterFacade(self._wb_context)

    def convert_ext_to_int(self, ext_item: ext.DashboardTabItem) -> IntTabItemLayoutItemPair:
        tab_item_dash_element_converter = self.create_tab_item_dash_element_converter_facade()

        return IntTabItemLayoutItemPair(
            tab_item_dash_element_converter.convert_ext_to_int(ext_item.element, id=ext_item.id),
            LayoutItemConverter.convert_ext_to_int(
                tab_item_id=ext_item.id,
                ext_layout_item=ext_item.placement,
            ),
        )

    def convert_int_to_ext(
            self,
            tab_item: dashboards.TabItem,
            layout_item: dashboards.LayoutItem,
    ) -> ext.DashboardTabItem:
        tab_item_dash_element_converter = self.create_tab_item_dash_element_converter_facade()

        placement, tab_item_id_from_layout_item = LayoutItemConverter.convert_int_to_ext(layout_item)
        assert tab_item_id_from_layout_item == tab_item.id

        return ext.DashboardTabItem(
            id=tab_item.id,
            placement=placement,
            element=tab_item_dash_element_converter.convert_int_to_ext(tab_item),
        )
