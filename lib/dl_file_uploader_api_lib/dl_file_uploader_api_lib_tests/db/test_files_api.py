import asyncio
import base64
import hashlib
import http
import json
import uuid

import aiohttp
import attr
import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_configs.crypto_keys import get_dummy_crypto_keys_config
from dl_constants.enums import FileProcessingStatus
from dl_file_uploader_api_lib.views.files import (
    S3_KEY_PARTS_SEPARATOR,
    MakePresignedUrlView,
)
from dl_file_uploader_api_lib_tests.req_builder import ReqBuilder
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.redis_model.models import DataFile
from dl_s3.data_sink import S3RawFileAsyncDataSink
from dl_s3.utils import upload_to_s3_by_presigned


@pytest.mark.asyncio
async def test_file_upload_cors(fu_client, s3_tmp_bucket, upload_file_req):
    resp = await fu_client.make_request(upload_file_req)
    assert resp.status == 201
    for cors_header in (
        "Access-Control-Expose-Headers",
        "Access-Control-Allow-Origin",
        "Access-Control-Allow-Credentials",
    ):
        assert cors_header in resp.headers


@pytest.mark.asyncio
async def test_make_presigned_url(fu_client, s3_tmp_bucket, rci):
    expected_url_fields = ("key", "x-amz-algorithm", "x-amz-credential", "x-amz-date", "policy", "x-amz-signature")

    resp = await fu_client.make_request(ReqBuilder.presigned_url("mymd5"))
    assert resp.status == 200
    assert "url" in resp.json, resp.json
    assert "fields" in resp.json, resp.json
    assert all(field in resp.json["fields"] for field in expected_url_fields), resp.json
    key = resp.json["fields"]["key"]
    key_parts = key.split(S3_KEY_PARTS_SEPARATOR)
    assert len(key_parts) == 2, key_parts
    assert key_parts[0] == rci.user_id


@pytest.mark.asyncio
async def test_download_presigned_url(fu_client, s3_tmp_bucket, csv_data):
    content_md5 = base64.b64encode(hashlib.md5(csv_data.encode("utf-8")).digest()).decode("utf-8")

    presigned_url_resp = await fu_client.make_request(ReqBuilder.presigned_url(content_md5))
    assert presigned_url_resp.status == 200, presigned_url_resp.json

    upload_resp = await upload_to_s3_by_presigned(presigned_url_resp.json, content_md5, csv_data)
    upload_resp_data = await upload_resp.read()
    assert upload_resp.status == 204, upload_resp_data

    download_resp = await fu_client.make_request(
        ReqBuilder.presigned_url_download(presigned_url_resp.json["fields"]["key"], "csv_data.csv")
    )
    assert download_resp.status == 201, download_resp.json


@pytest.mark.asyncio
async def test_download_presigned_url_bad_user(fu_client, s3_tmp_bucket, csv_data):
    content_md5 = base64.b64encode(hashlib.md5(csv_data.encode("utf-8")).digest()).decode("utf-8")

    presigned_url_resp = await fu_client.make_request(ReqBuilder.presigned_url(content_md5))
    assert presigned_url_resp.status == 200, presigned_url_resp.json

    _, file_uuid = presigned_url_resp.json["fields"]["key"].split(S3_KEY_PARTS_SEPARATOR)
    presigned_url_resp.json["fields"]["key"] = S3_KEY_PARTS_SEPARATOR.join(("hacker", file_uuid))

    download_resp = await fu_client.make_request(
        ReqBuilder.presigned_url_download(presigned_url_resp.json["fields"]["key"], "csv_data.csv", require_ok=False),
    )
    assert download_resp.status != 201, download_resp.json
    assert download_resp.json["code"] == "ERR.FILE.PERMISSION_DENIED"


@pytest.mark.asyncio
async def test_upload_presigned_too_large_file(monkeypatch, fu_client, s3_tmp_bucket, csv_data):
    monkeypatch.setattr(MakePresignedUrlView, "PRESIGNED_URL_MAX_BYTES", 32)

    content_md5 = base64.b64encode(hashlib.md5(csv_data.encode("utf-8")).digest()).decode("utf-8")

    presigned_url_resp = await fu_client.make_request(ReqBuilder.presigned_url(content_md5))
    assert presigned_url_resp.status == 200, presigned_url_resp.json

    with pytest.raises(aiohttp.ClientResponseError):
        await upload_to_s3_by_presigned(presigned_url_resp.json, content_md5, csv_data)


@pytest.mark.asyncio
async def test_upload_presigned_bad_key(monkeypatch, fu_client, s3_tmp_bucket, csv_data):
    content_md5 = base64.b64encode(hashlib.md5(csv_data.encode("utf-8")).digest()).decode("utf-8")

    presigned_url_resp = await fu_client.make_request(ReqBuilder.presigned_url(content_md5))
    assert presigned_url_resp.status == 200, presigned_url_resp.json

    presigned_url_resp.json["fields"]["key"] = "hacker/file"

    with pytest.raises(aiohttp.ClientResponseError):
        await upload_to_s3_by_presigned(presigned_url_resp.json, content_md5, csv_data)


@pytest.mark.asyncio
async def test_file_upload(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_cli,
    upload_file_req,
    csv_data,
):
    resp = await fu_client.make_request(upload_file_req)
    assert resp.status == 201
    file_id = resp.json["file_id"]
    assert file_id
    title = resp.json["title"]
    assert title

    redis_data = await redis_cli.get(DataFile._generate_key_by_id(file_id))
    decoded_data = json.loads(redis_data.decode())
    assert decoded_data["id"] == file_id
    # assert decoded_data['size'] == len(csv_data.encode('utf-8'))

    s3_obj = await s3_client.get_object(
        Bucket=s3_tmp_bucket,
        Key=file_id,
    )
    assert s3_obj
    s3_data = await s3_obj["Body"].read()
    assert s3_data.decode() == csv_data


@pytest.mark.asyncio
async def test_excel_upload(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_cli,
    upload_excel_req,
    excel_data,
    reader_app,
):
    resp = await fu_client.make_request(upload_excel_req)
    assert resp.status == 201
    file_id = resp.json["file_id"]
    assert file_id
    title = resp.json["title"]
    assert title

    redis_data = await redis_cli.get(DataFile._generate_key_by_id(file_id))
    decoded_data = json.loads(redis_data.decode())
    assert decoded_data["id"] == file_id

    s3_obj = await s3_client.get_object(
        Bucket=s3_tmp_bucket,
        Key=file_id,
    )
    assert s3_obj
    s3_data = await s3_obj["Body"].read()
    assert s3_data == excel_data


@pytest.mark.asyncio
async def test_too_large_file_upload(
    monkeypatch,
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_cli,
    upload_file_req_12mb,
    csv_data,
):
    monkeypatch.setattr(S3RawFileAsyncDataSink, "max_file_size_bytes", 8 * 1024**2)

    request = attr.evolve(upload_file_req_12mb, require_ok=False)
    resp = await fu_client.make_request(request)
    assert resp.status == http.HTTPStatus.REQUEST_ENTITY_TOO_LARGE, resp.content


@pytest.mark.skip("TODO")
@pytest.mark.asyncio
async def test_10MB_file_upload(
    fu_client,
    s3_client,
    s3_tmp_bucket,
    redis_model_manager,
    upload_file_req_10mb,
):
    resp = await fu_client.make_request(upload_file_req_10mb)
    assert resp.status == 201
    file_id = resp.json["file_id"]
    assert file_id

    await asyncio.sleep(3)

    df = await DataFile.get(manager=redis_model_manager, obj_id=file_id)
    assert df.sources


@pytest.mark.asyncio
async def test_file_status(fu_client, uploaded_file_id: str):
    file_id = uploaded_file_id
    resp = await fu_client.make_request(ReqBuilder.file_status(file_id))
    assert resp.status == 200
    assert resp.json["file_id"] == file_id
    assert resp.json["status"] in ("ready", "in_progress")
    assert resp.json["error"] is None


@pytest.mark.asyncio
async def test_file_sources(fu_client, uploaded_file_id):
    resp = await fu_client.make_request(ReqBuilder.file_sources(uploaded_file_id))
    assert resp.status == 200
    assert resp.json["file_id"] == uploaded_file_id
    assert isinstance(resp.json["sources"], list)
    source = resp.json["sources"][0]
    assert source["source_id"]
    assert source["title"]
    assert isinstance(source["is_applicable"], bool)


@pytest.mark.asyncio
async def test_file_404(fu_client):
    resp = await fu_client.make_request(ReqBuilder.file_sources(str(uuid.uuid4()), require_ok=False))
    assert resp.status == 404


@pytest.mark.asyncio
async def test_file_403(fu_client, redis_cli):
    rmm = RedisModelManager(
        redis=redis_cli,
        rci=RequestContextInfo(user_id="some_user_id_1"),
        crypto_keys_config=get_dummy_crypto_keys_config(),
    )
    df = DataFile(
        manager=rmm,
        filename="test_file.csv",
        status=FileProcessingStatus.in_progress,
    )
    await df.save()

    resp = await fu_client.make_request(ReqBuilder.file_sources(df.id, require_ok=False))
    assert resp.status == 403
