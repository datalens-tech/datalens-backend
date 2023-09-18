from bi_external_api.domain import external as ext
from bi_external_api.domain.external import (
    Dashboard,
    DashboardTab,
    DashboardTabItem,
    DashChartsContainer,
    DashInstance,
    DashTabItemPlacement,
    WidgetTab,
    WorkBook,
    WorkbookOpRequest,
    WorkbookWriteRequest,
    get_external_model_mapper,
)
from bi_external_api.enums import ExtAPIType


def test_write_wb_request_deserialize_widget_container_bw_field_name_support():
    dash_inst_1 = dict(
        name="dash_1",
        dashboard=dict(
            tabs=(
                dict(
                    id="123",
                    title="123 Title",
                    items=[
                        dict(
                            id="tiid_chart_0",
                            placement=dict(h=2, w=8, x=0, y=0),
                            element=dict(
                                kind="widget_container",  # legacy kind name
                                hide_title=True,
                                tabs=[
                                    dict(
                                        id="wtid_total_sales_indicator", chart_name="total_sales_indicator", title=None
                                    ),
                                ],
                                default_active_chart_tab_id="tiid_chart_0",
                            ),
                        ),
                    ],
                ),
                dict(
                    id="2",
                    title="2 Title",
                    items=[
                        dict(
                            id="tiid_chart_1",
                            placement=dict(h=1, w=5, x=10, y=10),
                            element=dict(
                                kind="charts_container",
                                hide_title=True,
                                tabs=[
                                    dict(
                                        id="wtid_total_sales_indicator2",
                                        chart_name="total_sales_indicator2",
                                        title=None,
                                    ),
                                ],
                                default_widget_id="tiid_chart_1",  # legacy field name
                            ),
                        ),
                    ],
                ),
            )
        ),
    )

    wb_body = dict(
        datasets=[],
        charts=[],
        dashboards=[
            dash_inst_1,
        ],
    )
    src = dict(
        kind="wb_modify",
        workbook_id="foobar",
        force_rewrite="False",
        workbook=wb_body,
    )
    schema_cls = get_external_model_mapper(ExtAPIType.CORE).get_or_create_schema_for_attrs_class(WorkbookOpRequest)
    schema = schema_cls()
    result = schema.load(src)

    expected = WorkbookWriteRequest(
        force_rewrite=False,
        workbook=WorkBook(
            datasets=(),
            charts=(),
            dashboards=(
                DashInstance(
                    name="dash_1",
                    dashboard=Dashboard(
                        tabs=(
                            DashboardTab(
                                id="123",
                                title="123 Title",
                                items=(
                                    DashboardTabItem(
                                        id="tiid_chart_0",
                                        element=DashChartsContainer(
                                            hide_title=True,
                                            tabs=(
                                                WidgetTab(
                                                    id="wtid_total_sales_indicator",
                                                    chart_name="total_sales_indicator",
                                                    title=None,
                                                ),
                                            ),
                                            default_active_chart_tab_id="tiid_chart_0",
                                        ),
                                        placement=DashTabItemPlacement(x=0, y=0, h=2, w=8),
                                    ),
                                ),
                            ),
                            DashboardTab(
                                id="2",
                                title="2 Title",
                                items=(
                                    DashboardTabItem(
                                        id="tiid_chart_1",
                                        element=DashChartsContainer(
                                            hide_title=True,
                                            tabs=(
                                                WidgetTab(
                                                    id="wtid_total_sales_indicator2",
                                                    chart_name="total_sales_indicator2",
                                                    title=None,
                                                ),
                                            ),
                                            default_active_chart_tab_id="tiid_chart_1",
                                        ),
                                        placement=DashTabItemPlacement(x=10, y=10, h=1, w=5),
                                    ),
                                ),
                            ),
                        )
                    ),
                ),
            ),
        ),
        workbook_id="foobar",
    )
    assert result.__dict__ == expected.__dict__


def test_dc_op_get_connection():
    data = ext.DCOpConnectionGetResponse(
        connection=ext.ConnectionInstance(
            name="2d062c77d6714c3abf17c25d50c6eadf",
            connection=ext.ClickHouseConnection(
                raw_sql_level=ext.RawSQLLevel.off,
                cache_ttl_sec=None,
                host="example.com",
                port=443,
                username="user",
                secure="on",
            ),
        ),
    )
    model_mapper = get_external_model_mapper(ExtAPIType.DC)

    schema_cls = model_mapper.get_schema_for_attrs_class(type(data))
    serialized = schema_cls().dump(data)

    expected = {
        "connection": {
            "connection": {
                "cache_ttl_sec": None,
                "host": "example.com",
                "kind": "clickhouse",
                "port": 443,
                "raw_sql_level": "off",
                "secure": "on",
                "username": "user",
            },
            "name": "2d062c77d6714c3abf17c25d50c6eadf",
        }
    }
    assert serialized == expected
