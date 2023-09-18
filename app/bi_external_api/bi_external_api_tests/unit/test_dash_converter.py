from __future__ import annotations

from typing import (
    Any,
    Optional,
    Sequence,
    Union,
)

import attr
import pytest

from bi_external_api.converter.charts.utils import convert_field_type_dataset_to_chart
from bi_external_api.converter.converter_exc import (
    CompositeConverterError,
    WorkbookEntryNotFound,
)
from bi_external_api.converter.dash import DashboardConverter
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    charts,
    dashboards,
)
from bi_external_api.domain.internal.dashboards import Connection
from bi_external_api.structs.mappings import FrozenMappingStrToStrOrStrSeq
from bi_external_api.structs.singleormultistring import SingleOrMultiString
from bi_external_api.testings import FlatTableChartBuilder

from .conftest import (
    PG_1_DS,
    PG_CONN,
    WB_ID,
)

FLAT_TABLE_ID = "flat_table_id"
FLAT_TABLE_NAME = "flat_table_name"

FLAT_TABLE_INST = (
    FlatTableChartBuilder()
    .dataset_instance(PG_1_DS)
    .all_dataset_fields_to_columns()
    .build_chart_instance(id=FLAT_TABLE_ID, name=FLAT_TABLE_NAME, wb_id=WB_ID)
)

COMMON_WB_CONTEXT = WorkbookContext(
    connections=[PG_CONN],
    datasets=[PG_1_DS],
    charts=[FLAT_TABLE_INST],
    dashboards=[],
)


@attr.s(auto_attribs=True)
class DashTestCase:
    name: str
    ext_dash: ext.Dashboard
    int_dash: dashboards.Dashboard

    @classmethod
    def ds_source_field_kwargs(cls, field_id: str) -> dict[str, Any]:
        ds_inst = PG_1_DS
        field = PG_1_DS.dataset.get_field_by_id(field_id)
        return dict(
            fieldType=field.data_type,
            datasetFieldType=convert_field_type_dataset_to_chart(field.type),
            datasetFieldId=field_id,
            datasetId=ds_inst.summary.id,
        )

    @classmethod
    def selector_case(
        cls,
        *,
        name: str,
        multiselect: bool,
        field_id: str,
        ext_default_value: Optional[ext.Value],
        int_default_value: Optional[SingleOrMultiString],
        ext_operation: ext.ComparisonOperation = None,
        int_operation: charts.Operation = None,
        param_default: Union[str, Sequence[str]],
    ) -> DashTestCase:
        title = f"{field_id.capitalize()} title"
        show_title = True

        ext_control_source = ext.ControlValueSourceDatasetField(
            dataset_name=PG_1_DS.summary.name,
            field_id=field_id,
        )
        ext_control: ext.DashControl

        if multiselect:
            ext_control = ext.DashControlMultiSelect(
                show_title=show_title,
                title=title,
                source=ext_control_source,
                default_value=ext_default_value,
                comparison_operation=ext_operation,
            )
        else:
            ext_control = ext.DashControlSelect(
                show_title=show_title,
                title=title,
                source=ext_control_source,
                default_value=ext_default_value,
                comparison_operation=ext_operation,
            )

        return DashTestCase.control_case(
            name=name,
            ext_control=ext_control,
            int_control=dashboards.ItemControl(
                id="--replace-me--",
                namespace="default",
                data=dashboards.DatasetBasedControlData(
                    title=title,
                    source=dashboards.DatasetControlSourceSelect(
                        showTitle=show_title,
                        multiselectable=multiselect,  # Front does not set `false` here
                        operation=int_operation,
                        defaultValue=int_default_value,
                        **cls.ds_source_field_kwargs(field_id),
                    ),
                ),
                defaults=FrozenMappingStrToStrOrStrSeq({field_id: param_default}),
            ),
        )

    @classmethod
    def text_input_case(
        cls,
        *,
        name: str,
        field_id: str,
        ext_default_value: Optional[ext.Value],
        int_default_value: Optional[SingleOrMultiString],
        ext_operation: ext.ComparisonOperation = None,
        int_operation: charts.Operation = None,
        param_default: Union[str, Sequence[str]],
    ) -> DashTestCase:
        title = f"{field_id.capitalize()} title"
        show_title = True

        return DashTestCase.control_case(
            name=name,
            ext_control=ext.DashControlTextInput(
                show_title=show_title,
                title=title,
                source=ext.ControlValueSourceDatasetField(
                    dataset_name=PG_1_DS.summary.name,
                    field_id=field_id,
                ),
                default_value=ext_default_value,
                comparison_operation=ext_operation,
            ),
            int_control=dashboards.ItemControl(
                id="--replace-me--",
                namespace="default",
                data=dashboards.DatasetBasedControlData(
                    title=title,
                    source=dashboards.DatasetControlSourceTextInput(
                        showTitle=show_title,
                        operation=int_operation,
                        defaultValue=int_default_value,
                        **cls.ds_source_field_kwargs(field_id),
                    ),
                ),
                defaults=FrozenMappingStrToStrOrStrSeq({field_id: param_default}),
            ),
        )

    @classmethod
    def control_case(
        cls,
        name: str,
        ext_control: ext.DashControl,
        int_control: dashboards.ItemControl,
    ) -> DashTestCase:
        tab_id = "sTa"
        tab_title = "Selector case tab title"
        tab_item_id = "TIid"
        ext_item_placement = ext.DashTabItemPlacement(x=0, y=0, w=1, h=1)
        int_item_placement = dashboards.LayoutItem(i=tab_item_id, x=0, y=0, w=1, h=1)

        return cls(
            name=name,
            ext_dash=ext.Dashboard(
                tabs=[
                    ext.DashboardTab(
                        id=tab_id,
                        title=tab_title,
                        ignored_connections=[
                            ext.IgnoredConnection(
                                from_id="foo",
                                to_id="bar",
                            ),
                        ],
                        items=[ext.DashboardTabItem(id=tab_item_id, placement=ext_item_placement, element=ext_control)],
                    )
                ],
            ),
            int_dash=dashboards.Dashboard(
                tabs=[
                    dashboards.Tab(
                        id=tab_id,
                        title=tab_title,
                        items=[attr.evolve(int_control, id=tab_item_id)],
                        layout=[int_item_placement],
                        connections=[
                            Connection(
                                from_="foo",
                                to="bar",
                                kind="ignore",
                            )
                        ],
                        aliases=dashboards.Aliases(default=[]),
                    )
                ],
            ),
        )

    @classmethod
    def single_element_case(cls, name: str, ext_elem: ext.DashElement, int_item: dashboards.TabItem) -> DashTestCase:
        tab_id = "sTa"
        tab_title = "Selector case tab title"
        tab_item_id = "TIid"
        ext_item_placement = ext.DashTabItemPlacement(x=0, y=0, w=1, h=1)
        int_item_placement = dashboards.LayoutItem(i=tab_item_id, x=0, y=0, w=1, h=1)

        return cls(
            name=name,
            ext_dash=ext.Dashboard(
                tabs=[
                    ext.DashboardTab(
                        id=tab_id,
                        title=tab_title,
                        items=[ext.DashboardTabItem(id=tab_item_id, placement=ext_item_placement, element=ext_elem)],
                        ignored_connections=[
                            ext.IgnoredConnection(
                                from_id="foo",
                                to_id="bar",
                            ),
                        ],
                    )
                ],
            ),
            int_dash=dashboards.Dashboard(
                tabs=[
                    dashboards.Tab(
                        id=tab_id,
                        title=tab_title,
                        items=[attr.evolve(int_item, id=tab_item_id)],
                        layout=[int_item_placement],
                        connections=[
                            Connection(
                                from_="foo",
                                to="bar",
                                kind="ignore",
                            )
                        ],
                        aliases=dashboards.Aliases(default=[]),
                    )
                ],
            ),
        )


CASES = [
    DashTestCase(
        name="simple_single_chart_dash",  # region
        ext_dash=ext.Dashboard(
            tabs=(
                ext.DashboardTab(
                    id="Ak",
                    title="Вкладка 1",
                    items=(
                        ext.DashboardTabItem(
                            id="chart_1",
                            placement=ext.DashTabItemPlacement(
                                h=2,
                                w=8,
                                x=0,
                                y=0,
                            ),
                            element=ext.DashChartsContainer(
                                hide_title=False,
                                tabs=[
                                    ext.WidgetTab(
                                        id="ma",
                                        title="tbl",
                                        chart_name=FLAT_TABLE_INST.summary.name,
                                    )
                                ],
                                default_active_chart_tab_id="ma",
                            ),
                        ),
                    ),
                    ignored_connections=[
                        ext.IgnoredConnection(
                            from_id="foo",
                            to_id="bar",
                        ),
                    ],
                ),
            ),
        ),
        int_dash=dashboards.Dashboard(
            tabs=(
                dashboards.Tab(
                    id="Ak",
                    title="Вкладка 1",
                    items=(
                        dashboards.ItemWidget(
                            id="chart_1",
                            namespace="default",
                            data=dashboards.TabItemDataWidget(
                                hideTitle=False,
                                tabs=(
                                    dashboards.WidgetTabItem(
                                        id="ma",
                                        title="tbl",
                                        params=FrozenMappingStrToStrOrStrSeq({}),
                                        chartId=FLAT_TABLE_INST.summary.id,
                                        isDefault=True,
                                        autoHeight=False,
                                        description="",
                                    ),
                                ),
                            ),
                        ),
                    ),
                    layout=(
                        dashboards.LayoutItem(
                            i="chart_1",
                            h=2,
                            w=8,
                            x=0,
                            y=0,
                        ),
                    ),
                    connections=[
                        Connection(
                            from_="foo",
                            to="bar",
                            kind="ignore",
                        )
                    ],
                    aliases=dashboards.Aliases(
                        default=(),
                    ),
                ),
            ),
        ),  # endregion
    ),
    DashTestCase(
        name="dataset_based_multiselect",  # region
        ext_dash=ext.Dashboard(
            tabs=(
                ext.DashboardTab(
                    id="Ak",
                    title="Вкладка 1",
                    items=(
                        ext.DashboardTabItem(
                            id="g5",
                            placement=ext.DashTabItemPlacement(h=10, w=12, x=0, y=2),
                            element=ext.DashChartsContainer(
                                hide_title=False,
                                tabs=[
                                    ext.WidgetTab(
                                        id="ma",
                                        title="tbl",
                                        chart_name=FLAT_TABLE_INST.summary.name,
                                    )
                                ],
                                default_active_chart_tab_id="ma",
                            ),
                        ),
                        ext.DashboardTabItem(
                            id="OK",
                            placement=ext.DashTabItemPlacement(h=2, w=12, x=0, y=0),
                            element=ext.DashControlMultiSelect(
                                title="customer title",
                                show_title=True,
                                source=ext.ControlValueSourceDatasetField(
                                    dataset_name=PG_1_DS.summary.name,
                                    field_id="customer",
                                ),
                                default_value=ext.MultiStringValue(values=["one", "two"]),
                            ),
                        ),
                    ),
                    ignored_connections=[
                        ext.IgnoredConnection(
                            from_id="foo",
                            to_id="bar",
                        ),
                    ],
                ),
            ),
        ),
        int_dash=dashboards.Dashboard(
            tabs=(
                dashboards.Tab(
                    id="Ak",
                    title="Вкладка 1",
                    items=(
                        dashboards.ItemWidget(
                            id="g5",
                            namespace="default",
                            data=dashboards.TabItemDataWidget(
                                hideTitle=False,
                                tabs=(
                                    dashboards.WidgetTabItem(
                                        id="ma",
                                        title="tbl",
                                        params=FrozenMappingStrToStrOrStrSeq({}),
                                        chartId=FLAT_TABLE_ID,
                                        isDefault=True,
                                        autoHeight=False,
                                        description="",
                                    ),
                                ),
                            ),
                        ),
                        dashboards.ItemControl(
                            id="OK",
                            namespace="default",
                            data=dashboards.DatasetBasedControlData(
                                title="customer title",
                                source=dashboards.DatasetControlSourceSelect(
                                    showTitle=True,
                                    multiselectable=True,
                                    defaultValue=SingleOrMultiString.from_sequence(
                                        [
                                            "one",
                                            "two",
                                        ]
                                    ),
                                    **DashTestCase.ds_source_field_kwargs("customer"),
                                ),
                            ),
                            defaults=FrozenMappingStrToStrOrStrSeq(
                                {
                                    "customer": (
                                        "one",
                                        "two",
                                    ),
                                }
                            ),
                        ),
                    ),
                    layout=(
                        dashboards.LayoutItem(
                            i="g5",
                            h=10,
                            w=12,
                            x=0,
                            y=2,
                        ),
                        dashboards.LayoutItem(
                            i="OK",
                            h=2,
                            w=12,
                            x=0,
                            y=0,
                        ),
                    ),
                    connections=[
                        Connection(
                            from_="foo",
                            to="bar",
                            kind="ignore",
                        )
                    ],
                    aliases=dashboards.Aliases(
                        default=(),
                    ),
                ),
            ),
        ),  # endregion
    ),
    DashTestCase.selector_case(
        name="selector_single_without_default",  # region
        multiselect=False,
        field_id="customer",
        ext_default_value=None,
        int_default_value=None,
        param_default="",
        # endregion
    ),
    DashTestCase.selector_case(
        name="selector_single_with_default",  # region
        multiselect=False,
        field_id="customer",
        ext_default_value=ext.SingleStringValue("Vasya"),
        int_default_value=SingleOrMultiString.from_string("Vasya"),
        param_default="Vasya",
        # endregion
    ),
    DashTestCase.selector_case(
        name="selector_single_without_default_custom_operation",  # region
        multiselect=False,
        field_id="customer",
        ext_operation=ext.ComparisonOperation.NE,
        int_operation=charts.Operation.NE,
        ext_default_value=None,
        int_default_value=None,
        param_default="",
        # endregion
    ),
    DashTestCase.selector_case(
        name="selector_single_with_default_custom_operation",  # region
        multiselect=False,
        field_id="customer",
        ext_operation=ext.ComparisonOperation.NE,
        int_operation=charts.Operation.NE,
        ext_default_value=ext.SingleStringValue("Vasya"),
        int_default_value=SingleOrMultiString.from_string("Vasya"),
        param_default="__ne_Vasya",
        # endregion
    ),
    DashTestCase.text_input_case(
        name="text_input_without_default",  # region
        field_id="amount",
        ext_default_value=None,
        # TODO FIX: BI-3005 WHY IT NOT ACCEPT NULL?!!!! IT even RETURNS NULL when is created via UI
        int_default_value=SingleOrMultiString.from_string(""),
        param_default="",
        # endregion
    ),
    DashTestCase.text_input_case(
        name="text_input_with_default",  # region
        field_id="amount",
        ext_default_value=ext.SingleStringValue("1"),
        int_default_value=SingleOrMultiString.from_string("1"),
        param_default="1",
        # endregion
    ),
    DashTestCase.text_input_case(
        name="text_input_without_default_custom_operation",  # region
        field_id="amount",
        ext_operation=ext.ComparisonOperation.GTE,
        int_operation=charts.Operation.GTE,
        ext_default_value=None,
        # TODO FIX: BI-3005 WHY IT NOT ACCEPT NULL?!!!! IT even RETURNS NULL when is created via UI
        int_default_value=SingleOrMultiString.from_string(""),
        param_default="",
        # endregion
    ),
    DashTestCase.text_input_case(
        name="selector_single_with_default_custom_operation",  # region
        field_id="amount",
        ext_operation=ext.ComparisonOperation.GTE,
        int_operation=charts.Operation.GTE,
        ext_default_value=ext.SingleStringValue("12"),
        int_default_value=SingleOrMultiString.from_string("12"),
        param_default="__gte_12",
        # endregion
    ),
    DashTestCase.selector_case(
        name="selector_multi_without_default",  # region
        multiselect=True,
        field_id="position",
        ext_default_value=None,
        int_default_value=None,
        param_default="",
        # endregion
    ),
    DashTestCase.selector_case(
        name="selector_multi_with_default",  # region
        multiselect=True,
        field_id="position",
        ext_default_value=ext.MultiStringValue(("Consumer", "Home Office")),
        int_default_value=SingleOrMultiString.from_sequence(("Consumer", "Home Office")),
        param_default=(
            "Consumer",
            "Home Office",
        ),
        # endregion
    ),
    DashTestCase.selector_case(
        name="selector_multi_without_default_custom_operation",  # region
        multiselect=True,
        field_id="position",
        ext_operation=ext.ComparisonOperation.NIN,
        int_operation=charts.Operation.NIN,
        ext_default_value=None,
        int_default_value=None,
        param_default="",
        # endregion
    ),
    DashTestCase.selector_case(
        name="selector_multi_with_default_custom_operation",  # region
        multiselect=True,
        field_id="position",
        ext_operation=ext.ComparisonOperation.NIN,
        int_operation=charts.Operation.NIN,
        ext_default_value=ext.MultiStringValue(("Consumer", "Home Office")),
        int_default_value=SingleOrMultiString.from_sequence(("Consumer", "Home Office")),
        param_default=(
            "__nin_Consumer",
            "__nin_Home Office",
        ),
        # endregion
    ),
    DashTestCase.control_case(
        name="selector_date_range_with_default",  # region
        ext_control=ext.DashControlDateRange(
            show_title=True,
            title="Order Date",
            source=ext.ControlValueSourceDatasetField(
                dataset_name=PG_1_DS.summary.name,
                field_id="date",
            ),
            default_value=ext.SingleStringValue("__interval___relative_-30d___relative_-0d"),
        ),
        int_control=dashboards.ItemControl(
            id="--replace-me--",
            namespace="default",
            data=dashboards.DatasetBasedControlData(
                title="Order Date",
                source=dashboards.DatasetControlSourceDate(
                    showTitle=True,
                    defaultValue=SingleOrMultiString(("__interval___relative_-30d___relative_-0d",), is_single=True),
                    isRange=True,
                    **DashTestCase.ds_source_field_kwargs("date"),
                ),
            ),
            defaults=FrozenMappingStrToStrOrStrSeq(
                {
                    "date": "__interval___relative_-30d___relative_-0d",
                }
            ),
        ),  # endregion
    ),
    DashTestCase.control_case(
        name="selector_date_range_without_default",  # region
        ext_control=ext.DashControlDateRange(
            show_title=True,
            title="Order Date",
            source=ext.ControlValueSourceDatasetField(
                dataset_name=PG_1_DS.summary.name,
                field_id="date",
            ),
            default_value=None,
        ),
        int_control=dashboards.ItemControl(
            id="--replace-me--",
            namespace="default",
            data=dashboards.DatasetBasedControlData(
                title="Order Date",
                source=dashboards.DatasetControlSourceDate(
                    showTitle=True,
                    # TODO FIX: BI-3005 WHY IT NOT ACCEPT NULL?!!!! IT even RETURNS NULL when is created via UI
                    defaultValue=SingleOrMultiString.from_string(""),
                    isRange=True,
                    **DashTestCase.ds_source_field_kwargs("date"),
                ),
            ),
            defaults=FrozenMappingStrToStrOrStrSeq(
                {
                    "date": "",
                }
            ),
        ),  # endregion
    ),
    DashTestCase.control_case(
        name="selector_date_without_default",  # region
        ext_control=ext.DashControlDate(
            show_title=True,
            title="Order Date",
            source=ext.ControlValueSourceDatasetField(
                dataset_name=PG_1_DS.summary.name,
                field_id="date",
            ),
            default_value=None,
        ),
        int_control=dashboards.ItemControl(
            id="--replace-me--",
            namespace="default",
            data=dashboards.DatasetBasedControlData(
                title="Order Date",
                source=dashboards.DatasetControlSourceDate(
                    showTitle=True,
                    # TODO FIX: BI-3005 WHY IT NOT ACCEPT NULL?!!!! IT even RETURNS NULL when is created via UI
                    defaultValue=SingleOrMultiString.from_string(""),
                    isRange=False,
                    **DashTestCase.ds_source_field_kwargs("date"),
                ),
            ),
            defaults=FrozenMappingStrToStrOrStrSeq(
                {
                    "date": "",
                }
            ),
        ),  # endregion
    ),
    DashTestCase.control_case(
        name="selector_date_with_default",  # region
        ext_control=ext.DashControlDate(
            show_title=True,
            title="Order Date",
            source=ext.ControlValueSourceDatasetField(
                dataset_name=PG_1_DS.summary.name,
                field_id="date",
            ),
            default_value=ext.SingleStringValue("2022-03-01T00:00:00.000Z"),
        ),
        int_control=dashboards.ItemControl(
            id="--replace-me--",
            namespace="default",
            data=dashboards.DatasetBasedControlData(
                title="Order Date",
                source=dashboards.DatasetControlSourceDate(
                    showTitle=True,
                    defaultValue=SingleOrMultiString(("2022-03-01T00:00:00.000Z",), is_single=True),
                    isRange=False,
                    **DashTestCase.ds_source_field_kwargs("date"),
                ),
            ),
            defaults=FrozenMappingStrToStrOrStrSeq(
                {
                    "date": "2022-03-01T00:00:00.000Z",
                }
            ),
        ),  # endregion
    ),
    DashTestCase.single_element_case(
        name="simple_text",  # region
        ext_elem=ext.DashText(text="My favorite text"),
        int_item=dashboards.ItemText(
            data=dashboards.TabItemDataText(text="My favorite text"), id="whatever"
        ),  # endregion
    ),
    DashTestCase.single_element_case(
        name="title_no_toc_m",  # region
        ext_elem=ext.DashTitle(
            text="My favorite text",
            size=ext.DashTitleTextSize.m,
            show_in_toc=False,
        ),
        int_item=dashboards.ItemTitle(
            data=dashboards.TabItemDataTitle(
                text="My favorite text",
                size=dashboards.TextSize.m,
                showInTOC=False,
            ),
            id="whatever",
        ),  # endregion
    ),
    DashTestCase.single_element_case(
        name="title_toc_xs",  # region
        ext_elem=ext.DashTitle(
            text="My favorite text",
            size=ext.DashTitleTextSize.xs,
            show_in_toc=True,
        ),
        int_item=dashboards.ItemTitle(
            data=dashboards.TabItemDataTitle(
                text="My favorite text",
                size=dashboards.TextSize.xs,
                showInTOC=True,
            ),
            id="whatever",
        ),  # endregion
    ),
]


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
def test_convert_ext_to_int(case: DashTestCase):
    converter = DashboardConverter(wb_context=COMMON_WB_CONTEXT)
    actual_int_dash = converter.convert_ext_to_int(case.ext_dash)
    assert actual_int_dash == case.int_dash


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
def test_convert_int_to_ext(case: DashTestCase):
    converter = DashboardConverter(wb_context=COMMON_WB_CONTEXT)
    actual_ext_dash = converter.convert_int_to_ext(case.int_dash)
    assert actual_ext_dash == case.ext_dash


SAMPLE_EXT_DASH_FOR_DEFAULTER = ext.Dashboard(
    tabs=(
        ext.DashboardTab(
            id="tab1",
            title="Tab 1",
            items=(
                ext.DashboardTabItem(
                    id="chart_1",
                    placement=ext.DashTabItemPlacement(h=1, w=1, x=0, y=0),
                    element=ext.DashChartsContainer(
                        hide_title=False,
                        tabs=[
                            ext.WidgetTab(
                                id="wt1",
                                chart_name=FLAT_TABLE_INST.summary.name,
                            ),
                            ext.WidgetTab(
                                id="wt2",
                                title="Duplicate",
                                chart_name=FLAT_TABLE_INST.summary.name,
                            ),
                        ],
                    ),
                ),
            ),
            ignored_connections=[
                ext.IgnoredConnection(
                    from_id="foo",
                    to_id="bar",
                ),
            ],
        ),
    ),
)


def test_dash_widget_container_defaulter_ext():
    ext_dash = SAMPLE_EXT_DASH_FOR_DEFAULTER
    assert ext_dash.tabs[0].items[0].element.default_active_chart_tab_id is None
    dash = DashboardConverter(wb_context=COMMON_WB_CONTEXT).fill_ext_defaults(ext_dash)
    assert dash.tabs[0].items[0].element.default_active_chart_tab_id == "wt1"


def test_dash_widget_tab_defaulter_ext():
    ext_dash = SAMPLE_EXT_DASH_FOR_DEFAULTER
    assert ext_dash.tabs[0].items[0].element.tabs[0].title is None
    dash = DashboardConverter(wb_context=COMMON_WB_CONTEXT).fill_ext_defaults(ext_dash)
    assert dash.tabs[0].items[0].element.tabs[0].title == FLAT_TABLE_INST.summary.name


def test_exc_composition():
    def single_chart_wc(wtiid: str, chart_name: str) -> ext.DashChartsContainer:
        return ext.DashChartsContainer(
            hide_title=False,
            tabs=[
                ext.WidgetTab(
                    id=wtiid,
                    title=f"The {wtiid}",
                    chart_name=chart_name,
                )
            ],
            default_active_chart_tab_id="wtiid",
        )

    dash = ext.Dashboard(
        tabs=(
            ext.DashboardTab(
                id="tab_1",
                title="Tab 1",
                items=(
                    ext.DashboardTabItem(
                        id="wc_1",
                        element=single_chart_wc("cc_1", "UNKNOWN_CHART_1"),
                        placement=ext.DashTabItemPlacement(x=1, y=1, w=1, h=1),
                    ),
                    ext.DashboardTabItem(
                        id="wc_2",
                        element=single_chart_wc("cc_2", "UNKNOWN_CHART_2"),
                        placement=ext.DashTabItemPlacement(x=2, y=1, w=1, h=1),
                    ),
                ),
                ignored_connections=[
                    ext.IgnoredConnection(
                        from_id="foo",
                        to_id="bar",
                    ),
                ],
            ),
            ext.DashboardTab(
                id="tab_2",
                title="Tab 2",
                items=(
                    ext.DashboardTabItem(
                        id="wc_3",
                        element=single_chart_wc("cc_2", "UNKNOWN_CHART_3"),
                        placement=ext.DashTabItemPlacement(x=1, y=1, w=1, h=1),
                    ),
                ),
                ignored_connections=[
                    ext.IgnoredConnection(
                        from_id="foo",
                        to_id="bar",
                    ),
                ],
            ),
        )
    )
    converter = DashboardConverter(wb_context=COMMON_WB_CONTEXT)

    with pytest.raises(CompositeConverterError) as exc_info:
        converter.convert_ext_to_int(dash)

    exc: CompositeConverterError = exc_info.value
    actual_err_path_to_err_type_mapping = {path: type(nested_exc) for path, nested_exc in exc.data.map_path_exc.items()}
    assert actual_err_path_to_err_type_mapping == {
        ("tabs", "tab_1", "wc_1"): WorkbookEntryNotFound,
        ("tabs", "tab_1", "wc_2"): WorkbookEntryNotFound,
        ("tabs", "tab_2", "wc_3"): WorkbookEntryNotFound,
    }
