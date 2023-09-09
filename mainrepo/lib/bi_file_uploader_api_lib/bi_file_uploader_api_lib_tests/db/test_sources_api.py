import pytest
from bi_constants.api_constants import DLHeadersCommon
from bi_file_uploader_lib.redis_model.models import DataFile

from bi_file_uploader_api_lib_tests.req_builder import ReqBuilder


@pytest.mark.asyncio
async def test_file_status(fu_client, redis_model_manager, uploaded_file_id: str):
    file_id = uploaded_file_id
    df = await DataFile.get(manager=redis_model_manager, obj_id=file_id)
    source_id = df.sources[0].id

    resp = await fu_client.make_request(ReqBuilder.source_status(file_id, source_id))
    assert resp.status == 200
    assert resp.json["file_id"] == file_id
    assert resp.json["source_id"] == source_id
    assert resp.json["status"] in ("ready", "in_progress")
    assert resp.json["error"] is None


@pytest.mark.asyncio
async def test_source_info(fu_client, uploaded_file_id: str):
    file_id = uploaded_file_id

    sources_resp = await fu_client.make_request(ReqBuilder.file_sources(uploaded_file_id))
    source_id = sources_resp.json["sources"][0]["source_id"]

    resp = await fu_client.make_request(ReqBuilder.source_info(file_id, source_id))
    assert resp.status == 200
    assert resp.json["file_id"] == file_id
    assert isinstance(resp.json["source"], dict)
    source = resp.json["source"]
    assert source["source_id"] == source_id
    assert source["title"]
    assert source["is_valid"]
    assert source["raw_schema"]
    assert source["preview"]
    assert "error" in source
    assert resp.json["options"]
    assert resp.json["data_settings"]
    assert all(setting in resp.json["data_settings"] for setting in ("encoding", "delimiter", "first_line_is_header"))

    resp1 = await fu_client.make_request(
        ReqBuilder.source_info(
            file_id=file_id,
            source_id=source_id,
            data_json={
                "column_types": [
                    {"name": "f2", "user_type": "float"},
                    {"name": "data", "user_type": "genericdatetime"},
                    {"name": "data_i_vremya", "user_type": "string"},
                ],
            },
        )
    )
    assert resp1.status == 200


@pytest.mark.asyncio
async def test_apply_settings(fu_client, redis_model_manager, uploaded_file_id: str):
    file_id = uploaded_file_id
    df = await DataFile.get(manager=redis_model_manager, obj_id=file_id)
    source_id = df.sources[0].id

    resp_info = await fu_client.make_request(
        ReqBuilder.source_info(
            file_id=file_id,
            source_id=source_id,
        )
    )
    previous_title = resp_info.json["source"]["title"]

    resp = await fu_client.make_request(
        ReqBuilder.apply_source_settings(
            file_id=file_id,
            source_id=source_id,
            data_json={
                "data_settings": {
                    "encoding": "utf8",
                    "delimiter": "comma",
                    "first_line_is_header": False,
                },
                "title": "new_title.csv",
            },
        )
    )
    assert resp.status == 200

    resp_info = await fu_client.make_request(
        ReqBuilder.source_info(
            file_id=file_id,
            source_id=source_id,
        )
    )
    new_title = resp_info.json["source"]["title"]
    assert new_title != previous_title
    assert new_title == "new_title.csv"

    resp = await fu_client.make_request(
        ReqBuilder.apply_source_settings(
            file_id=file_id,
            source_id=source_id,
            data_json={
                "data_settings": {
                    "encoding": "utf8",
                    "delimiter": "comma",
                    "first_line_is_header": False,
                },
            },
        )
    )
    assert resp.status == 200

    resp_info = await fu_client.make_request(
        ReqBuilder.source_info(
            file_id=file_id,
            source_id=source_id,
        )
    )
    newest_title = resp_info.json["source"]["title"]
    assert newest_title == new_title


@pytest.mark.asyncio
async def test_source_preview(fu_client, master_token_header, uploaded_file_id: str, redis_model_manager):
    file_id = uploaded_file_id

    sources_resp = await fu_client.make_request(ReqBuilder.file_sources(file_id))
    source_id = sources_resp.json["sources"][0]["source_id"]
    df = await DataFile.get(manager=redis_model_manager, obj_id=file_id)
    source = df.get_source_by_id(source_id)
    preview_id = source.preview_id

    resp = await fu_client.make_request(
        ReqBuilder.preview(
            file_id,
            source_id,
            master_token_header,
            data_json={
                "preview_id": preview_id,
                "raw_schema": [
                    {"user_type": "string", "name": "f1"},
                    {"user_type": "integer", "name": "f2"},
                    {"user_type": "string", "name": "f3"},
                    {"user_type": "date", "name": "data"},
                    {"user_type": "genericdatetime", "name": "data_i_vremya"},
                ],
            },
        )
    )
    assert resp.status == 200
    assert resp.json["preview"] == [
        ["qwe", "123", "45.9", "2021-02-04", "2021-02-04T12:00:00"],
        ["asd", "345", "47.9", "2021-02-05", "2021-02-05T14:01:00"],
        ["zxc", "456", "49,9", "2021-02-06", "2021-02-06T11:59:00"],
        ["zxc", "456", None, None, "2021-02-06T11:59:00"],
        ["zxczxczxcz...czxczxczxc", "456", "49.9", "2021-02-06", None],
    ]


@pytest.mark.asyncio
async def test_excel_source(
    master_token_header,
    fu_client,
    uploaded_excel_id: str,
    redis_model_manager,
    reader_app,
):
    file_id = uploaded_excel_id

    sources_resp = await fu_client.make_request(ReqBuilder.file_sources(file_id))
    source_id = sources_resp.json["sources"][0]["source_id"]
    df = await DataFile.get(manager=redis_model_manager, obj_id=file_id)
    source = df.get_source_by_id(source_id)
    preview_id = source.preview_id

    resp = await fu_client.make_request(
        ReqBuilder.preview(
            file_id,
            source_id,
            master_token_header,
            data_json={
                "preview_id": preview_id,
                "raw_schema": [
                    {"user_type": "integer", "name": "row"},
                    {"user_type": "string", "name": "order_id"},
                    {"user_type": "genericdatetime", "name": "order_date"},
                    {"user_type": "genericdatetime", "name": "ship_date"},
                    {"user_type": "string", "name": "ship_mode"},
                    {"user_type": "string", "name": "customer_id"},
                    {"user_type": "string", "name": "customer_name"},
                    {"user_type": "string", "name": "segment"},
                    {"user_type": "string", "name": "city"},
                    {"user_type": "string", "name": "state"},
                    {"user_type": "string", "name": "global_area"},
                    {"user_type": "string", "name": "postal_code"},
                    {"user_type": "string", "name": "market"},
                    {"user_type": "string", "name": "region"},
                    {"user_type": "string", "name": "product_id"},
                    {"user_type": "string", "name": "category"},
                    {"user_type": "string", "name": "sub-category"},
                    {"user_type": "string", "name": "product_name"},
                    {"user_type": "float", "name": "sales"},
                    {"user_type": "integer", "name": "quantity"},
                    {"user_type": "float", "name": "discount"},
                    {"user_type": "float", "name": "profit"},
                    {"user_type": "float", "name": "shipping_cost"},
                    {"user_type": "string", "name": "order_priority"},
                    {"user_type": "float", "name": "ammount"},
                ],
            },
        )
    )
    assert resp.status == 200
    assert resp.json["preview"][0] == [
        "1",
        "MX-2014-143658",
        "2014-10-02T00:00:00",
        "2014-10-06T00:00:00",
        "Standard Class",
        "SC-20575",
        "Sonia Cooley",
        "Consumer",
        "Mexico City",
        "Distrito Federal",
        "Mexico",
        None,
        "LATAM",
        "North",
        "OFF-LA-10002782",
        "Office Supplies",
        "Labels",
        "Hon File Folder Labels, Adjustable",
        "13.08",
        "3",
        "0.0",
        "4.56",
        "1.033",
        "Medium",
        "39.24",
    ]

    resp = await fu_client.make_request(ReqBuilder.source_info(file_id, source_id))
    assert resp.status == 200
    assert resp.json["file_id"] == file_id
    assert resp.json["options"] is None
    assert isinstance(resp.json["source"], dict)
    source = resp.json["source"]
    assert source["source_id"] == source_id
    assert source["title"]
    assert source["is_valid"]
    assert source["raw_schema"]
    assert source["preview"]
    assert "error" in source
    assert resp.json["data_settings"]
    assert "first_line_is_header" in resp.json["data_settings"]


@pytest.mark.asyncio
async def test_source_raw_schema(fu_client, master_token_header, uploaded_file_id: str):
    file_id = uploaded_file_id

    sources_resp = await fu_client.make_request(ReqBuilder.file_sources(file_id))
    source_id = sources_resp.json["sources"][0]["source_id"]

    resp = await fu_client.make_request(ReqBuilder.internal_params(file_id, source_id, master_token_header))
    assert resp.status == 200
    assert resp.json["raw_schema"][2]["user_type"] == "float"

    resp = await fu_client.make_request(
        ReqBuilder.internal_params(
            file_id,
            source_id,
            master_token_header,
            data_json={
                "raw_schema": [
                    {"user_type": "string", "name": "f3"},
                ],
            },
        )
    )
    assert resp.status == 200
    assert resp.json["raw_schema"][2]["user_type"] == "string"


@pytest.mark.asyncio
async def test_invalid_master_token(fu_client):
    resp = await fu_client.make_request(
        ReqBuilder.internal_params(
            "-",
            "-",
            master_token_header={DLHeadersCommon.FILE_UPLOADER_MASTER_TOKEN: "invalid-master-token"},
            require_ok=False,
        )
    )
    assert resp.status == 403


@pytest.mark.asyncio
async def test_no_master_token(fu_client):
    resp = await fu_client.make_request(
        ReqBuilder.internal_params(
            "-",
            "-",
            master_token_header=None,
            require_ok=False,
        )
    )
    assert resp.status == 403
