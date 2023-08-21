import pytest

from bi_external_api.domain import external as ext
from bi_external_api.testings import WorkbookOpsClient
from bi_external_api.workbook_ops.public_exceptions import WorkbookOperationException
from bi_testing_ya.api_wrappers import Req


@pytest.mark.asyncio
async def test_ping(bi_ext_api_client):
    resp = await bi_ext_api_client.make_request(Req("get", "ping"))
    assert resp.status == 200


@pytest.mark.asyncio
async def test_rpc_validation(bi_ext_api_client):
    resp = await bi_ext_api_client.make_request(
        Req(method="POST", url="external_api/v0/workbook/rpc", data_json={}, require_ok=False)
    )
    assert resp.json == {
        "kind": "request_scheme_violation",
        "messages": {
            "kind": ["Missing data for required field."]
        }
    }
    assert resp.status == 400


@pytest.mark.asyncio
async def test_error(bi_ext_api_client: WorkbookOpsClient):
    with pytest.raises(WorkbookOperationException) as exc_info:
        await bi_ext_api_client.read_workbook(ext.WorkbookReadRequest(
            workbook_id="Non-existing"
        ))

    exc: WorkbookOperationException = exc_info.value
    exc_data = exc.data
    assert exc_data.request_id

    # TODO FIX: Check messages when become stabilized
    assert len(exc_data.common_errors) == 1
    assert len(exc_data.entry_errors) == 0
    assert exc_data.partial_workbook is None

    # TODO: rework fixtures to parametrize test with and without _do_add_exc_message
    common_error = exc_data.common_errors[0]
    # assert "Traceback (most recent call last):" in common_error.stacktrace
    assert "Object not found" in common_error.exc_message
    assert "operation=\'workbook_info_get\'" in common_error.exc_message


@pytest.mark.asyncio
async def test_get_empty_workbook(
        bi_ext_api_client: WorkbookOpsClient,
        pseudo_wb_path,
        pg_connection,  # Just to create WBl
):
    wb_read_resp = await bi_ext_api_client.read_workbook(ext.WorkbookReadRequest(
        workbook_id=pseudo_wb_path
    ))
    assert wb_read_resp.workbook == ext.WorkBook.create_empty()
