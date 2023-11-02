import asyncio
import logging

import pytest

from dl_constants.enums import FileProcessingStatus
from dl_file_uploader_api_lib_tests.req_builder import ReqBuilder
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    GSheetsUserSourceProperties,
    YaDocsUserSourceProperties,
)


LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_yadocuments_public_file(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
    reader_app,
):
    public_link = "https://disk.yandex.ru/i/ZgabI6zyoYn8IQ"
    resp = await fu_client.make_request(ReqBuilder.upload_documents(public_link=public_link))
    assert resp.status == 201
    file_id = resp.json["file_id"]
    assert file_id

    await asyncio.sleep(5)

    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=file_id)
    assert isinstance(df, DataFile)
    assert isinstance(df.user_source_properties, YaDocsUserSourceProperties)
    assert df.status == FileProcessingStatus.ready
    assert df.error is None
    assert df.user_source_properties.public_link == public_link
    assert df.filename == "test public table.xlsx"
    for sheet_src in df.sources:
        assert sheet_src.status == FileProcessingStatus.ready
        assert sheet_src.error is None


@pytest.mark.asyncio
async def test_yadocuments_private_file(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
    reader_app,
    ya_docs_oauth_token,
):
    private_path = "test private table.xlsx"
    resp = await fu_client.make_request(
        ReqBuilder.upload_documents(private_path=private_path, oauth_token=ya_docs_oauth_token)
    )
    assert resp.status == 201
    file_id = resp.json["file_id"]
    assert file_id

    await asyncio.sleep(5)

    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=file_id)
    assert isinstance(df, DataFile)
    assert isinstance(df.user_source_properties, YaDocsUserSourceProperties)
    assert df.status == FileProcessingStatus.ready
    assert df.error is None
    assert df.user_source_properties.private_path == private_path
    assert df.filename == "test private table.xlsx"
    for sheet_src in df.sources:
        assert sheet_src.status == FileProcessingStatus.ready
        assert sheet_src.error is None


@pytest.mark.asyncio
async def test_yadocuments_invalid_link(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
    reader_app,
):
    public_link_invalid = "https://disk.yandeks.ru/i/ZgabI6zyoYn8IQ"
    resp = await fu_client.make_request(ReqBuilder.upload_documents(public_link=public_link_invalid, require_ok=False))
    assert resp.status == 400
    assert resp.json["code"] == "ERR.FILE.INVALID_LINK"


@pytest.mark.asyncio
async def test_yadocuments_not_found(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
    reader_app,
):
    public_link_nonexisten = "https://disk.yandex.ru/i/fffffffff"
    resp = await fu_client.make_request(ReqBuilder.upload_documents(public_link=public_link_nonexisten))
    assert resp.status == 201
    file_id = resp.json["file_id"]

    await asyncio.sleep(2)

    resp = await fu_client.make_request(ReqBuilder.file_status(file_id))
    assert resp.status == 200
    assert resp.json["file_id"] == file_id
    assert resp.json["status"] == "failed"
    assert resp.json["error"]["code"] == "ERR.FILE.NOT_FOUND"


@pytest.mark.asyncio
async def test_documents_unsupported(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
    reader_app,
):
    public_link_to_docs = "https://disk.yandex.ru/i/ros0GDegLEpyew"
    resp = await fu_client.make_request(ReqBuilder.upload_documents(public_link=public_link_to_docs))
    assert resp.status == 201
    file_id = resp.json["file_id"]

    await asyncio.sleep(2)

    resp = await fu_client.make_request(ReqBuilder.file_status(file_id))
    assert resp.status == 200
    assert resp.json["file_id"] == file_id
    assert resp.json["status"] == "failed"
    assert resp.json["error"]["code"] == "ERR.FILE.UNSUPPORTED_DOCUMENT"
