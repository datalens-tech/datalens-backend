import attr

from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import dashboards
from .defaulter import ExtDashChartContainerDefaultWidgetIdDefaulter, ExtDashWidgetTabDefaulter
from .tab_item import BaseDashboardItemConverter, IntTabItemLayoutItemPair
from ..converter_exc_composer import ConversionErrHandlingContext
from ...domain.internal.dashboards import Connection


@attr.s()
class DashboardConverter:
    _wb_context: WorkbookContext = attr.ib()

    @classmethod
    def fill_ext_defaults(cls, dash: ext.Dashboard) -> ext.Dashboard:
        dash = ExtDashChartContainerDefaultWidgetIdDefaulter().process(dash)
        dash = ExtDashWidgetTabDefaulter().process(dash)
        return dash

    def create_dashboard_item_converter(self) -> BaseDashboardItemConverter:
        return BaseDashboardItemConverter(wb_context=self._wb_context)

    def convert_tab_ext_to_int(
        self,
        ext_tab: ext.DashboardTab,
        *,
        exc_handling_ctx: ConversionErrHandlingContext
    ) -> dashboards.Tab:
        dashboard_item_converter = self.create_dashboard_item_converter()

        tab_item_layout_item_pair_list: list[IntTabItemLayoutItemPair] = []

        for tab_item in ext_tab.items:
            with exc_handling_ctx.postpone_error_with_path(tab_item.id):
                tab_item_layout_item_pair_list.append(
                    dashboard_item_converter.convert_ext_to_int(tab_item)
                )

        connections = [
            Connection(
                from_=ic.from_id,
                to=ic.to_id,
                kind="ignore",
            )
            for ic in ext_tab.ignored_connections or []
        ]

        return dashboards.Tab(
            id=ext_tab.id,
            title=ext_tab.title,
            aliases=dashboards.Aliases(
                default=(),
            ),
            connections=connections,
            items=[
                tab_item_layout_item_pair.tab_item
                for tab_item_layout_item_pair in tab_item_layout_item_pair_list
            ],
            layout=[
                tab_item_layout_item_pair.layout_item
                for tab_item_layout_item_pair in tab_item_layout_item_pair_list
            ],
        )

    def convert_tab_int_to_ext(self, int_tab: dashboards.Tab) -> ext.DashboardTab:
        dashboard_item_converter = self.create_dashboard_item_converter()

        ext_tab_items = [
            dashboard_item_converter.convert_int_to_ext(
                tab_item=int_tab_item,
                layout_item=next(layout_item for layout_item in int_tab.layout if layout_item.i == int_tab_item.id)
            )
            for int_tab_item in int_tab.items
        ]

        ignored_connections = [
            ext.IgnoredConnection(
                from_id=conn.from_,
                to_id=conn.to,
            )
            for conn in int_tab.connections
            if conn.kind == "ignore"
        ]

        return ext.DashboardTab(
            id=int_tab.id,
            title=int_tab.title,
            items=ext_tab_items,
            ignored_connections=ignored_connections,
        )

    def convert_ext_to_int(self, ext_dash: ext.Dashboard) -> dashboards.Dashboard:
        with ConversionErrHandlingContext().cm() as exc_ctx:
            with exc_ctx.push_path("tabs"):
                int_tabs: list[dashboards.Tab] = []

                for ext_tab in ext_dash.tabs:
                    with exc_ctx.postpone_error_with_path(ext_tab.id):
                        int_tabs.append(self.convert_tab_ext_to_int(ext_tab, exc_handling_ctx=exc_ctx))

        return dashboards.Dashboard(
            tabs=int_tabs,
        )

    def convert_int_to_ext(self, int_dash: dashboards.Dashboard) -> ext.Dashboard:
        return ext.Dashboard(
            tabs=[
                self.convert_tab_int_to_ext(int_tab)
                for int_tab in int_dash.tabs
            ]
        )
