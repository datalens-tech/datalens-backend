from uuid import uuid4

import pytest

from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.main import DatasetConverter
from bi_external_api.domain import external as ext
from bi_external_api.domain.external import get_external_model_mapper
from bi_external_api.enums import ExtAPIType
from bi_external_api_tests.test_acceptance import (
    _test_dc_connection_create_modify_delete,
    _test_dc_workbook_create_modify_delete,
)
from bi_testing_ya.api_wrappers import Req


@pytest.mark.asyncio
async def test_get_empty_workbook(
    bi_ext_api_client,
    pseudo_wb_path,
    pg_connection,  # Just to create US folder
):
    resp = await bi_ext_api_client.make_request(
        Req(method="GET", url=f"/external_api/v0/workbook/instance/{pseudo_wb_path}")
    )
    assert resp.status == 200
    print(resp.json)
    assert resp.json == {
        "id": None,
        "project_id": None,
        "title": None,
        "workbook": {"dashboards": [], "datasets": [], "charts": []},
    }


@pytest.mark.asyncio
async def test_single_dataset_in_workbook(
    bi_ext_api_client,
    pseudo_wb_path,
    dataset_factory,
    wb_ctx_loader,
):
    int_dataset_instance = await dataset_factory.create_dataset(
        "testy",
        query="SELECT t.* FROM (VALUES(1, 'one'), (2, 'two')) AS t (num, txt)",
    )

    wb_ctx = await wb_ctx_loader.load(pseudo_wb_path)
    ds_converter = DatasetConverter(wb_ctx, ConverterContext())
    expected_ext_dataset = ds_converter.convert_internal_dataset_to_public_dataset(int_dataset_instance.dataset)
    ext_ds_schema = get_external_model_mapper(ExtAPIType.YA_TEAM).get_or_create_schema_for_attrs_class(ext.Dataset)()
    expected_ext_dataset_dict = ext_ds_schema.dump(expected_ext_dataset)

    resp = await bi_ext_api_client.make_request(
        Req(method="GET", url=f"/external_api/v0/workbook/instance/{pseudo_wb_path}")
    )
    assert resp.status == 200
    print(resp.json)
    assert resp.json == {
        "id": None,
        "project_id": None,
        "title": None,
        "workbook": {
            "dashboards": [],
            "datasets": [
                {
                    "name": int_dataset_instance.summary.name,
                    "dataset": expected_ext_dataset_dict,
                }
            ],
            "charts": [],
        },
    }


@pytest.mark.asyncio
async def test_dc_workbook_create_modify_delete(
    bi_ext_api_dc_client,
):
    client = bi_ext_api_dc_client
    wb_title = uuid4().hex
    await _test_dc_workbook_create_modify_delete(
        client=client,
        wb_title=wb_title,
        project_id="dummy_project_id",
    )


# TODO FIX: BI-4282 Support project ID resolution in dataset-api service fixture
#  (add functionality to trust auth middleware)
@pytest.mark.skip("Currently dataset-api in fixtures does not support pass-throw of project ID")
@pytest.mark.asyncio
async def test_connection_create_modify_delete(
    bi_ext_api_dc_client,
    bi_ext_api_dc_tmp_wb_id,
):
    client = bi_ext_api_dc_client
    wb_id = bi_ext_api_dc_tmp_wb_id
    conn_name = uuid4().hex
    await _test_dc_connection_create_modify_delete(client, wb_id, conn_name)
