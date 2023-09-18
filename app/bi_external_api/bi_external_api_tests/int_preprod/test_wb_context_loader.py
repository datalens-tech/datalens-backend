import time

import attr
import pytest

from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.converter.workbook_ctx_loader import WorkbookContextLoader
from bi_external_api.converter.workbook_gathering_ctx import IDNameNormalizer
from bi_external_api.domain.internal import (
    charts,
    dashboards,
)
from bi_external_api.domain.internal.dl_common import EntrySummary
from bi_external_api.internal_api_clients.exc_api import WorkbookNotFound
from bi_external_api.testings import (
    FlatTableChartBuilder,
    PGSubSelectDatasetFactory,
    SingleTabDashboardBuilder,
)
from dl_testing.utils import skip_outside_devhost


# TODO FIX: Move this test to db section when charts-api will be available in docker
@skip_outside_devhost
@pytest.mark.asyncio
async def test_wb_ctx_loader(
    bi_ext_api_int_preprod_charts_api_client,
    bi_ext_api_int_preprod_dash_api_client,
    wb_ctx_loader: WorkbookContextLoader,
    dataset_factory: PGSubSelectDatasetFactory,
    pg_connection,
    pseudo_wb_path,
):
    charts_cli = bi_ext_api_int_preprod_charts_api_client
    dash_api_cli = bi_ext_api_int_preprod_dash_api_client

    # Creating dataset
    ds_inst = await dataset_factory.create_dataset(
        "testy",
        query="SELECT t.* FROM (VALUES(1, 'one'), (2, 'two')) AS t (num, txt)",
    )

    # Creating chart
    chart = (FlatTableChartBuilder(dataset_inst=ds_inst).dataset_fields_to_columns("num", "txt")).build_chart()

    chart_name = "tlb_chart"
    chart_summary = await charts_cli.create_chart(chart, workbook_id=pseudo_wb_path, name=chart_name)

    # Creating dashboard
    dash = (
        SingleTabDashboardBuilder()
        .tab_id("the_tab_id")
        .tab_title("the_tab_title")
        .item_selector_dataset_based(
            dash_tab_item_id="s_1",
            title="TXT",
            dataset_inst=ds_inst,
            default_values=["one"],
            field_id="txt",
            pp=SingleTabDashboardBuilder.PP(x=0, y=0, w=1, h=1),
        )
        .item_widget_single_tab(
            dash_tab_item_id="wc_1",
            title="Table num txt",
            chart_id=chart_summary.id,
            widget_tab_item_id="wc_1_tid",
            pp=SingleTabDashboardBuilder.PP(x=0, y=1, w=2, h=2),
        )
    ).build_dash()

    dash_name = "Main dash"
    dash_summary = await dash_api_cli.create_dashboard(dash, workbook_id=pseudo_wb_path, name=dash_name)

    loaded_ctx = await wb_ctx_loader.load(pseudo_wb_path)
    expected_chart_instance = charts.ChartInstance(chart=chart, summary=chart_summary)
    expected_dash_instance = dashboards.DashInstance(dash=dash, summary=dash_summary)

    assert loaded_ctx == WorkbookContext(
        connections=frozenset({pg_connection}),
        datasets=[ds_inst],
        charts=[expected_chart_instance],
        dashboards=[expected_dash_instance],
    )
    assert loaded_ctx.resolve_chart_by_name(chart_name) == expected_chart_instance


@skip_outside_devhost
@pytest.mark.skip
@pytest.mark.asyncio
async def test_wb_ctx_loader(wb_ctx_loader):
    wb = await wb_ctx_loader.load("alpha_workbooks/my_wb_1")
    assert wb.load_fail_info_collection


@skip_outside_devhost
@pytest.mark.asyncio
async def test_wb_ctx_loader_wb_not_found(wb_ctx_loader):
    with pytest.raises(WorkbookNotFound):
        wb = await wb_ctx_loader.load(f"non_existing_dir_{time.time()}/wb")


@skip_outside_devhost
@pytest.mark.asyncio
async def test_wb_ctx_loader_gather_dash_summaries(pseudo_wb_path, wb_ctx_loader, dashes_in_hierarchy):
    dash_seq = await wb_ctx_loader.gather_dash_summaries(us_path=pseudo_wb_path)

    assert set(dash_seq) == dashes_in_hierarchy


@skip_outside_devhost
@pytest.mark.asyncio
async def test_wb_ctx_loader_gather_workbook(pseudo_wb_path, wb_ctx_loader, dashes_in_hierarchy: list[EntrySummary]):
    wb_ctx, map_id_orig_summary = await wb_ctx_loader.gather_workbook_by_dash(
        dash_id_list=[],
        us_folder_path=pseudo_wb_path,
        name_normalizer_cls=IDNameNormalizer,
    )

    def normalize_inst_summary(summary: EntrySummary) -> EntrySummary:
        wb_id = "ephemeral"
        return attr.evolve(
            summary,
            workbook_id=wb_id,
            name=summary.id,
        )

    assert {dash_inst.summary for dash_inst in wb_ctx.dashboards} == {
        normalize_inst_summary(dash_summary) for dash_summary in dashes_in_hierarchy
    }
    assert map_id_orig_summary == {dash_summary.id: dash_summary for dash_summary in dashes_in_hierarchy}
