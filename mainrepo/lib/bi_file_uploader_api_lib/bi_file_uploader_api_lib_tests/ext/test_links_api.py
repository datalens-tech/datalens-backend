import asyncio
import json
import logging
import os

import pytest

from bi_constants.enums import FileProcessingStatus
from bi_core.aio.web_app_services.gsheets import GSheetsSettings
from bi_file_uploader_api_lib_tests.req_builder import ReqBuilder
from bi_file_uploader_lib.gsheets_client import GSheetsClient
from bi_file_uploader_lib.redis_model.models import (
    DataFile,
    GSheetsUserSourceProperties,
)
from bi_utils.aio import ContextVarExecutor

LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_gsheets(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
):
    spreadsheet_id = "1rnUFa7AiSKD5O80IKCvMy2cSZvLU1kRw9dxbtZbDMWc"
    resp = await fu_client.make_request(ReqBuilder.upload_gsheet(spreadsheet_id))
    assert resp.status == 201
    file_id = resp.json["file_id"]
    assert file_id

    await asyncio.sleep(5)

    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=file_id)
    assert isinstance(df, DataFile)
    assert isinstance(df.user_source_properties, GSheetsUserSourceProperties)
    assert df.user_source_properties.spreadsheet_id == spreadsheet_id
    assert df.filename == "fu"
    assert df.status == FileProcessingStatus.ready
    assert df.sources[0].status == FileProcessingStatus.failed
    assert all(source.status == FileProcessingStatus.ready for source in df.sources[1:])

    resp = await fu_client.make_request(ReqBuilder.file_sources(file_id))
    assert resp.status == 200
    assert resp.json["sources"][0]["error"]["code"] == "ERR.FILE.NO_DATA"

    resp = await fu_client.make_request(ReqBuilder.source_info(file_id, df.sources[0].id))
    assert resp.status == 200
    assert resp.json["source"]["error"]["code"] == "ERR.FILE.NO_DATA"

    resp = await fu_client.make_request(ReqBuilder.source_info(file_id, df.sources[3].id))  # "elaborate" sheet
    assert resp.status == 200
    assert resp.json["options"] is None
    assert resp.json["data_settings"] == {"first_line_is_header": True}
    source = resp.json["source"]
    assert source["source_id"] == df.sources[3].id
    assert source["title"]
    assert source["is_valid"]
    assert source["raw_schema"]
    assert source["preview"]
    assert "error" in source
    assert source["spreadsheet_id"] == spreadsheet_id
    assert source["sheet_id"] == df.sources[3].user_source_dsrc_properties.sheet_id
    assert source["raw_schema"] == [
        {"name": "A", "user_type": "integer", "title": "int field"},
        {"name": "B", "user_type": "date", "title": "date field"},
        {"name": "C", "user_type": "float", "title": "float field"},
        {"name": "D", "user_type": "string", "title": "string field"},
        {"name": "E", "user_type": "boolean", "title": "bool field"},
        {"name": "F", "user_type": "genericdatetime", "title": "datetime field"},
        {"name": "G", "user_type": "string", "title": "time field"},
        {"name": "H", "user_type": "string", "title": "time period field"},
    ]
    assert source["preview"] == [
        ["1", "2001-07-31", "12.5", "asdf", "False", "2022-08-12T12:30:32", "12:00:12", "13:00:12"],
        ["9", "2022-08-11", None, "qwer", "True", "2022-08-13T12:30:32", "13:00:12", "14:00:12"],
        ["2", "1995-04-12", "12.4", "zxcv", "False", "2022-08-14T12:30:32", "14:00:12", "15:00:12"],
        ["8", "2008-02-29", None, "12", "True", "2022-08-15T12:30:32", "15:00:12", "16:00:12"],
        ["3", "2012-03-12", "12.3", "dfgh", "False", "2022-08-16T12:30:32", "16:00:12", "17:00:12"],
        [None, None, None, None, None, None, None, "18:00:12"],
    ]


@pytest.mark.asyncio
async def test_big_gsheet(
    use_local_task_processor,
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
):
    spreadsheet_id = "1sPIpWUZa7wgUnUDa-MFPidJTSjQ40jS2OmD0g6V_wDw"
    resp = await fu_client.make_request(ReqBuilder.upload_gsheet(spreadsheet_id))
    assert resp.status == 201
    file_id = resp.json["file_id"]
    assert file_id

    await asyncio.sleep(5)

    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=file_id)
    assert isinstance(df, DataFile)
    assert isinstance(df.user_source_properties, GSheetsUserSourceProperties)
    assert df.user_source_properties.spreadsheet_id == spreadsheet_id
    assert df.filename == "MoscowShops v8 (truncated)"
    assert df.status == FileProcessingStatus.ready
    assert len(df.sources) == 5
    assert all(source.status == FileProcessingStatus.ready for source in df.sources)

    resp = await fu_client.make_request(ReqBuilder.source_info(file_id, df.sources[0].id))
    assert resp.status == 200
    assert resp.json["options"] is None
    assert resp.json["data_settings"] == {"first_line_is_header": True}
    source = resp.json["source"]
    assert source["source_id"] == df.sources[0].id
    assert source["title"]
    assert source["is_valid"]
    assert source["raw_schema"]
    assert source["preview"]
    assert "error" in source
    assert source["spreadsheet_id"] == spreadsheet_id
    assert source["sheet_id"] == df.sources[0].user_source_dsrc_properties.sheet_id
    assert len(source["raw_schema"]) == 26


@pytest.mark.asyncio
async def test_gsheets_invalid_link(fu_client, s3_client, s3_tmp_bucket, redis_model_manager):
    resp = await fu_client.make_request(
        ReqBuilder.upload_gsheet(
            url="https://sheets.googleapis.com/v4/spreadsheets/1vFysKzPgy7Zw09K6wShVQPe6VXIoAzpGH3g_4X8dgyw",
            require_ok=False,
        )
    )
    assert resp.status == 400
    assert resp.json["code"] == "ERR.FILE.INVALID_LINK"


@pytest.mark.asyncio
async def test_gsheets_no_access_to_spreadsheet(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
):
    resp = await fu_client.make_request(ReqBuilder.upload_gsheet("1vFysKzPgy7Zw09K6wShVQPe6VXIoAzpGH3g_4X8dgyw"))
    assert resp.status == 201
    file_id = resp.json["file_id"]

    await asyncio.sleep(2)

    resp = await fu_client.make_request(ReqBuilder.file_status(file_id))
    assert resp.status == 200
    assert resp.json["file_id"] == file_id
    assert resp.json["status"] == "failed"
    assert resp.json["error"]["code"] == "ERR.FILE.PERMISSION_DENIED"


@pytest.mark.asyncio
async def test_gsheets_not_found(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
):
    resp = await fu_client.make_request(ReqBuilder.upload_gsheet("ffffffffffffffffffffffffffffffffffffffffffff"))
    assert resp.status == 201
    file_id = resp.json["file_id"]

    await asyncio.sleep(2)

    resp = await fu_client.make_request(ReqBuilder.file_status(file_id))
    assert resp.status == 200
    assert resp.json["file_id"] == file_id
    assert resp.json["status"] == "failed"
    assert resp.json["error"]["code"] == "ERR.FILE.NOT_FOUND"


@pytest.mark.asyncio
async def test_gsheets_unsupported_document(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
):
    resp = await fu_client.make_request(ReqBuilder.upload_gsheet("14wGijvJtpF6M3_S-gh-nctYsj50jfstm"))
    assert resp.status == 201
    file_id = resp.json["file_id"]

    await asyncio.sleep(2)

    resp = await fu_client.make_request(ReqBuilder.file_status(file_id))
    assert resp.status == 200
    assert resp.json["file_id"] == file_id
    assert resp.json["status"] == "failed"
    assert resp.json["error"]["code"] == "ERR.FILE.UNSUPPORTED_DOCUMENT"


@pytest.mark.asyncio
async def test_all_types(
    use_local_task_processor,
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
    env_param_getter,
):
    """
    Note: the spreadsheet in this test contains test data on it's _TEST_DATA_ sheet,
    which is filled in manually or generated with subsequent manual verification
    """

    # use this flag o print actual test data to fill the _TEST_DATA_ sheet when something changes in parsing,
    # be sure to check generated data manually
    print_actual = False

    spreadsheet_id = "1vTeWZz2M8c3bvTdRIrqsNhN9m4YwjUguwAnAqBahxi0"
    resp = await fu_client.make_request(ReqBuilder.upload_gsheet(spreadsheet_id))
    assert resp.status == 201
    file_id = resp.json["file_id"]
    assert file_id

    await asyncio.sleep(5)

    rmm = redis_model_manager
    df: DataFile = await DataFile.get(manager=rmm, obj_id=file_id)
    assert isinstance(df.user_source_properties, GSheetsUserSourceProperties)
    assert df.status == FileProcessingStatus.ready
    assert all(source.status == FileProcessingStatus.ready for source in df.sources)

    loaded_test_data = {}
    actual_data = {}

    gsheets_settings = GSheetsSettings(
        api_key=env_param_getter.get_str_value("GOOGLE_API_KEY"),
        client_id="dummy",
        client_secret="dummy",
    )
    auth = None
    with ContextVarExecutor(max_workers=min(32, os.cpu_count() * 3 + 4)) as tpe:
        async with GSheetsClient(settings=gsheets_settings, auth=auth, tpe=tpe) as sheets_client:
            spreadsheet = await sheets_client.get_spreadsheet(spreadsheet_id=spreadsheet_id)
    test_data_sheet = spreadsheet.sheets[0]
    test_value_keys = [cell.value for cell in test_data_sheet.data[0][1:]]
    for sheet_test_data_cells in test_data_sheet.data[1:]:
        sheet_title = sheet_test_data_cells[0].value
        loaded_test_data[sheet_title] = {}
        for test_value_key, test_value_cell in zip(test_value_keys, sheet_test_data_cells[1:]):
            loaded_test_data[sheet_title][test_value_key] = json.loads(test_value_cell.value)

    if print_actual:
        for test_value_key in test_value_keys:
            actual_data[test_value_key] = []
    for src in df.sources[1:]:
        resp = await fu_client.make_request(ReqBuilder.source_info(file_id, src.id))
        assert resp.status == 200
        source = resp.json["source"]
        assert source["title"]
        assert source["is_valid"]

        gsheet_title = source["title"].split()[-1]
        loaded_sheet_test_data = loaded_test_data[gsheet_title]
        for test_value_key, test_value in loaded_sheet_test_data.items():
            if not print_actual:
                assert source[test_value_key] == test_value, f"Failed on sheet {gsheet_title} checking {test_value_key}"
                LOGGER.info(f"{test_value_key} for {gsheet_title} is OK")
            else:
                actual_data[test_value_key].append(json.dumps(source[test_value_key], separators=(", ", ": ")))

    if print_actual:
        for test_value_key, actual_test_data in actual_data.items():
            print(f"\n\n\n{test_value_key}", "\n".join(actual_test_data), sep="\n")
