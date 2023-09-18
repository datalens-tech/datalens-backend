import abc
from typing import (
    Generic,
    TypeVar,
    final,
)

import attr

from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import dashboards
from bi_external_api.structs.mappings import FrozenMappingStrToStrOrStrSeq

_TAB_ITEM_TV = TypeVar("_TAB_ITEM_TV", bound=dashboards.TabItem)
_DASH_ELEMENT_TV = TypeVar("_DASH_ELEMENT_TV", bound=ext.DashElement)


@attr.s(frozen=True)
class BaseDashElementConverter(Generic[_TAB_ITEM_TV, _DASH_ELEMENT_TV], metaclass=abc.ABCMeta):
    _wb_context: WorkbookContext = attr.ib()

    @abc.abstractmethod
    def convert_ext_to_int(self, dash_element: _DASH_ELEMENT_TV, id: str) -> _TAB_ITEM_TV:
        raise NotImplementedError()

    @final
    def convert_int_to_ext(self, tab_item: _TAB_ITEM_TV) -> _DASH_ELEMENT_TV:
        assert tab_item.namespace == "default"
        return self._convert_int_to_ext(tab_item)

    @abc.abstractmethod
    def _convert_int_to_ext(self, tab_item: _TAB_ITEM_TV) -> _DASH_ELEMENT_TV:
        raise NotImplementedError()


class TitleDashElementConverter(BaseDashElementConverter[dashboards.ItemTitle, ext.DashTitle]):
    @staticmethod
    def _convert_text_size_int_to_ext(int_text_size: dashboards.TextSize) -> ext.DashTitleTextSize:
        return ext.DashTitleTextSize[int_text_size.name]

    @staticmethod
    def _convert_text_size_ext_to_int(ext_text_size: ext.DashTitleTextSize) -> dashboards.TextSize:
        return dashboards.TextSize[ext_text_size.name]

    def convert_ext_to_int(self, dash_element: ext.DashTitle, id: str) -> dashboards.ItemTitle:
        return dashboards.ItemTitle(
            data=dashboards.TabItemDataTitle(
                text=dash_element.text,
                showInTOC=dash_element.show_in_toc,
                size=self._convert_text_size_ext_to_int(dash_element.size),
            ),
            id=id,
        )

    def _convert_int_to_ext(self, tab_item: dashboards.ItemTitle) -> ext.DashTitle:
        return ext.DashTitle(
            text=tab_item.data.text,
            show_in_toc=tab_item.data.showInTOC or False,
            size=self._convert_text_size_int_to_ext(tab_item.data.size),
        )


class TextDashElementConverter(BaseDashElementConverter[dashboards.ItemText, ext.DashText]):
    def convert_ext_to_int(self, dash_element: ext.DashText, id: str) -> dashboards.ItemText:
        return dashboards.ItemText(
            data=dashboards.TabItemDataText(
                text=dash_element.text,
            ),
            id=id,
        )

    def _convert_int_to_ext(self, tab_item: dashboards.ItemText) -> ext.DashText:
        return ext.DashText(text=tab_item.data.text)


class WidgetContainerElementConverter(BaseDashElementConverter[dashboards.ItemWidget, ext.DashChartsContainer]):
    def convert_ext_to_int(self, dash_element: ext.DashChartsContainer, id: str) -> dashboards.ItemWidget:
        return dashboards.ItemWidget(
            data=dashboards.TabItemDataWidget(
                hideTitle=dash_element.hide_title,
                tabs=[
                    dashboards.WidgetTabItem(
                        chartId=self._wb_context.resolve_chart_by_name(widget_tab.chart_name).summary.id,
                        id=widget_tab.id,
                        # TODO FIX: Move defaulting logic to dedicated layer
                        title=widget_tab.title or widget_tab.chart_name,
                        # TODO FIX: Convert parameters
                        params=FrozenMappingStrToStrOrStrSeq({}),
                        isDefault=widget_tab.id == dash_element.default_active_chart_tab_id,
                        # autoHeight=None,
                        # description=None,
                    )
                    for widget_tab in dash_element.tabs
                ],
            ),
            id=id,
        )

    def _convert_int_to_ext(self, tab_item: dashboards.ItemWidget) -> ext.DashChartsContainer:
        default_tab_candidates = [tab for tab in tab_item.data.tabs if tab.isDefault]

        # TODO FIX: BI-3005 ask frontend why so much pain?
        #  https://datalens-preprod.yandex-team.ru/p0pv9wiwpyyqh-dash-2?state=2550f56086
        #  Got 2 default tabs
        # assert len(default_tab_candidates) == 1, "Got non-single default tab in widget container"

        default_tab = default_tab_candidates[0]

        return ext.DashChartsContainer(
            hide_title=tab_item.data.hideTitle,
            default_active_chart_tab_id=default_tab.id,
            tabs=[
                ext.WidgetTab(
                    chart_name=self._wb_context.resolve_chart_by_id(tab.chartId).summary.name,
                    id=tab.id,
                    # TODO FIX: Clarify None meaning
                    title=tab.title,
                )
                for tab in tab_item.data.tabs
            ],
        )
