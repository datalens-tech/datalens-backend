import attr

from bi_external_api.converter.converter_exc import NotSupportedYet, MalformedEntryConfig
from bi_external_api.converter.dash import dash_element_control_guided
from bi_external_api.converter.dash.dash_element_common import (
    BaseDashElementConverter,
    WidgetContainerElementConverter,
    TitleDashElementConverter,
    TextDashElementConverter,
)
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import dashboards


@attr.s()
class DashElementConverterFacade:
    _wb_context: WorkbookContext = attr.ib()

    def create_element_converter_for_element(
            self,
            element: ext.DashElement
    ) -> BaseDashElementConverter:
        if isinstance(element, ext.DashChartsContainer):
            return WidgetContainerElementConverter(self._wb_context)

        elif isinstance(element, ext.DashTitle):
            return TitleDashElementConverter(self._wb_context)

        elif isinstance(element, ext.DashText):
            return TextDashElementConverter(self._wb_context)

        elif isinstance(element, ext.DashControlMultiSelect):
            return dash_element_control_guided.MultiSelectorElementConverter(self._wb_context)

        elif isinstance(element, ext.DashControlSelect):
            return dash_element_control_guided.SingleValueSelectorElementConverter(self._wb_context)

        elif isinstance(element, ext.DashControlDateRange):
            return dash_element_control_guided.DateRangeElementConverter(self._wb_context)

        elif isinstance(element, ext.DashControlDate):
            return dash_element_control_guided.SingleDateElementConverter(self._wb_context)

        elif isinstance(element, ext.DashControlTextInput):
            return dash_element_control_guided.TextInputElementConverter(self._wb_context)

        elif isinstance(element, ext.DashElement):
            raise NotSupportedYet(f"Dash element {element.kind.name!r} is not yet supported")

        raise AssertionError(
            f"Got unexpected type of dash element in create_element_converter_for_element(): {type(element)}"
        )

    def create_element_converter_for_tab_item(
            self,
            tab_item: dashboards.TabItem,
    ) -> BaseDashElementConverter:
        if isinstance(tab_item, dashboards.ItemWidget):
            return WidgetContainerElementConverter(self._wb_context)

        if isinstance(tab_item, dashboards.ItemControl):
            values_source = tab_item.data.source

            if isinstance(values_source, dashboards.FieldSetCommonControlSourceSelect):
                if values_source.multiselectable:
                    return dash_element_control_guided.MultiSelectorElementConverter(self._wb_context)
                else:
                    return dash_element_control_guided.SingleValueSelectorElementConverter(self._wb_context)

            if isinstance(values_source, dashboards.FieldSetCommonControlSourceDate):
                if values_source.isRange:
                    return dash_element_control_guided.DateRangeElementConverter(self._wb_context)
                else:
                    return dash_element_control_guided.SingleDateElementConverter(self._wb_context)

            if isinstance(values_source, dashboards.FieldSetCommonControlSourceTextInput):
                return dash_element_control_guided.TextInputElementConverter(self._wb_context)

            raise MalformedEntryConfig(f"Dash control {type(values_source)} is not supported")

        if isinstance(tab_item, dashboards.ItemTitle):
            return TitleDashElementConverter(self._wb_context)

        if isinstance(tab_item, dashboards.ItemText):
            return TextDashElementConverter(self._wb_context)

        raise MalformedEntryConfig(f"Dashboard tab item {tab_item.type!r} is not supported")

    def convert_ext_to_int(self, element: ext.DashElement, id: str) -> dashboards.TabItem:
        converter = self.create_element_converter_for_element(element)
        return converter.convert_ext_to_int(element, id=id)

    def convert_int_to_ext(self, tab_item: dashboards.TabItem) -> ext.DashElement:
        converter = self.create_element_converter_for_tab_item(tab_item)
        return converter.convert_int_to_ext(tab_item)
