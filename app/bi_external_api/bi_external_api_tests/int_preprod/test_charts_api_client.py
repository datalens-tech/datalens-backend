import pytest

from bi_external_api.attrs_model_mapper import pretty_repr
from bi_external_api.domain.internal import charts
from bi_external_api.testings import (
    FlatTableChartBuilder,
    PGSubSelectDatasetFactory,
)
from dl_testing.utils import skip_outside_devhost


@skip_outside_devhost
@pytest.mark.asyncio
async def test_create_simple_chart(
    dataset_factory: PGSubSelectDatasetFactory,
    bi_ext_api_int_preprod_charts_api_client,
    pseudo_wb_path,
):
    charts_cli = bi_ext_api_int_preprod_charts_api_client

    ds_inst = await dataset_factory.create_dataset(
        "testy",
        query="SELECT t.* FROM (VALUES(1, 'one'), (2, 'two')) AS t (num, txt)",
    )

    chart = (FlatTableChartBuilder(dataset_inst=ds_inst).dataset_fields_to_columns("num", "txt")).build_chart()

    await charts_cli.create_chart(chart, workbook_id=pseudo_wb_path, name="tbl")


@skip_outside_devhost
@pytest.mark.skip
@pytest.mark.asyncio
async def test_get_chart(
    bi_ext_api_int_preprod_int_api_clients,
):
    """Test to quickly get chart config"""
    chart_cli = bi_ext_api_int_preprod_int_api_clients.charts
    chart_inst = await chart_cli.get_chart("4e6w43c28wqsw")
    print(pretty_repr(chart_inst.chart, preferred_cls_name_prefixes=[charts]))
