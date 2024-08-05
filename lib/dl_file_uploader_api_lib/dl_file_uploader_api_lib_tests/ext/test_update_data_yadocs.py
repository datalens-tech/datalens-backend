import asyncio
import datetime
import itertools
import logging
import uuid

import pytest

from dl_constants.enums import (
    FileProcessingStatus,
    UserDataType,
)
from dl_core.db import SchemaColumn
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core_testing.connection import make_conn_key
from dl_file_uploader_api_lib_tests.req_builder import ReqBuilder
from dl_file_uploader_lib import exc
from dl_testing.s3_utils import s3_file_exists

from dl_connector_bundle_chs3.chs3_yadocs.core.constants import CONNECTION_TYPE_YADOCS
from dl_connector_bundle_chs3.chs3_yadocs.core.lifecycle import YaDocsFileS3ConnectionLifecycleManager
from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection


LOGGER = logging.getLogger(__name__)


@pytest.fixture(scope="function")
async def saved_yadocs_connection(loop, bi_context, default_async_usm_per_test, s3_persistent_bucket, s3_client):
    us_manager = default_async_usm_per_test
    conn_name = "yadocs test conn {}".format(uuid.uuid4())
    long_long_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        seconds=YaDocsFileS3ConnectionLifecycleManager.STALE_THRESHOLD_SECONDS + 60,  # just in case
    )

    dummy_raw_schema = [SchemaColumn("dummy_column", user_type=UserDataType.string)]
    data = YaDocsFileS3Connection.DataModel(
        sources=[
            YaDocsFileS3Connection.FileDataSource(  # this is a valid source
                id=f"source_1_{uuid.uuid4()}",
                file_id=str(uuid.uuid4()),
                title="Source 1",
                status=FileProcessingStatus.ready,
                s3_filename=f"src_1_filename_{uuid.uuid4()}",
                raw_schema=dummy_raw_schema,
                public_link="https://disk.yandex.ru/i/zr3TzLtWlTjYnw",
                sheet_id="elaborate",
                first_line_is_header=True,
                data_updated_at=long_long_ago,
            ),
            YaDocsFileS3Connection.FileDataSource(  # this sheet has no data
                id=f"source_2_{uuid.uuid4()}",
                file_id=str(uuid.uuid4()),
                title="Source 2",
                status=FileProcessingStatus.ready,
                s3_filename=f"src_2_filename_{uuid.uuid4()}",
                raw_schema=dummy_raw_schema,
                public_link="https://disk.yandex.ru/i/zr3TzLtWlTjYnw",
                sheet_id="image",
                first_line_is_header=True,
                data_updated_at=long_long_ago,
            ),
            YaDocsFileS3Connection.FileDataSource(  # this sheet does not exist
                id=f"source_3_{uuid.uuid4()}",
                file_id=str(uuid.uuid4()),
                title="Source 3",
                status=FileProcessingStatus.ready,
                s3_filename=f"src_3_filename_{uuid.uuid4()}",
                raw_schema=dummy_raw_schema,
                public_link="https://disk.yandex.ru/i/zr3TzLtWlTjYnw",
                sheet_id="hello, world",
                first_line_is_header=True,
                data_updated_at=long_long_ago,
            ),
            YaDocsFileS3Connection.FileDataSource(  # this sheet's whole spreadsheet does not exist
                id=f"source_4_{uuid.uuid4()}",
                file_id=str(uuid.uuid4()),
                title="Source 4",
                status=FileProcessingStatus.ready,
                s3_filename=f"src_4_filename_{uuid.uuid4()}",
                raw_schema=dummy_raw_schema,
                public_link="https://disk.yandex.ru/i/1nxUuqeIoBvihQvvvv",
                sheet_id="99999999999",
                first_line_is_header=True,
                data_updated_at=long_long_ago,
            ),
            YaDocsFileS3Connection.FileDataSource(  # this is a valid source, but it failed during the previous update
                id=f"source_5_{uuid.uuid4()}",
                file_id=str(uuid.uuid4()),
                title="Source 5",
                status=FileProcessingStatus.failed,
                s3_filename=f"src_5_filename_{uuid.uuid4()}",  # removed below after file upload
                raw_schema=dummy_raw_schema,
                public_link="https://disk.yandex.ru/i/zr3TzLtWlTjYnw",
                sheet_id="elaborate",
                first_line_is_header=True,
                data_updated_at=long_long_ago,
            ),
            YaDocsFileS3Connection.FileDataSource(  # no access
                id=f"source_6_{uuid.uuid4()}",
                file_id=str(uuid.uuid4()),
                title="Source 6",
                status=FileProcessingStatus.ready,
                s3_filename=f"src_6_filename_{uuid.uuid4()}",
                raw_schema=dummy_raw_schema,
                public_link="https://disk.yandex.ru/i/1nxUuqeIoBvihQ",
                sheet_id="0",
                first_line_is_header=True,
                data_updated_at=long_long_ago,
            ),
        ]
    )
    conn = YaDocsFileS3Connection.create_from_dict(
        data,
        ds_key=make_conn_key("connections", conn_name),
        type_=CONNECTION_TYPE_YADOCS.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=us_manager,
    )
    await us_manager.save(conn)

    csv_data = "f1,f2,f3\nqwe,123,45.9\nasd,345,47.9".encode("utf-8")
    for src in conn.data.sources:
        await s3_client.put_object(
            ACL="private",
            Bucket=s3_persistent_bucket,
            Key=src.s3_filename,
            Body=csv_data,
        )

    for src in conn.data.sources:  # make sure all files are intact before the update
        assert await s3_file_exists(s3_client, s3_persistent_bucket, src.s3_filename)

    # conn.data.sources[4].s3_filename = None
    await us_manager.save(conn)

    yield conn

    updated_conn: YaDocsFileS3Connection = await us_manager.get_by_id(conn.uuid, expected_type=YaDocsFileS3Connection)
    for src in itertools.chain(
        conn.data.sources, updated_conn.data.sources
    ):  # cleanup original and updated files if any
        if src.s3_filename is not None:
            await s3_client.delete_object(
                Bucket=s3_persistent_bucket,
                Key=src.s3_filename,
            )
    await us_manager.delete(conn)


@pytest.mark.asyncio
async def test_update_connection_data_with_save(
    fu_client,
    redis_cli,
    default_async_usm_per_test: AsyncUSManager,
    s3_client,
    s3_tmp_bucket,
    s3_persistent_bucket,
    saved_yadocs_connection: YaDocsFileS3Connection,
    reader_app,
):
    conn = saved_yadocs_connection
    usm = default_async_usm_per_test
    data_updated_at_orig = conn.data.oldest_data_update_time(exclude_statuses={FileProcessingStatus.in_progress})

    resp = await fu_client.make_request(ReqBuilder.update_conn_data(conn, save=True, file_type="yadocs"))
    assert resp.status == 200

    await asyncio.sleep(5)

    updated_conn: YaDocsFileS3Connection = await usm.get_by_id(conn.uuid, expected_type=YaDocsFileS3Connection)

    new_data_updated_at = updated_conn.data.oldest_data_update_time(exclude_statuses={FileProcessingStatus.in_progress})
    assert data_updated_at_orig != new_data_updated_at

    src_1 = updated_conn.get_file_source_by_id(conn.data.sources[0].id)  # this is a valid source
    assert src_1.file_id != conn.data.sources[0].file_id
    assert await s3_file_exists(s3_client, s3_persistent_bucket, conn.data.sources[0].s3_filename) is False
    assert await s3_file_exists(s3_client, s3_persistent_bucket, updated_conn.data.sources[0].s3_filename) is True
    assert src_1.status == FileProcessingStatus.ready
    assert src_1.data_updated_at != data_updated_at_orig
    assert src_1.raw_schema is not None

    src_2 = updated_conn.get_file_source_by_id(conn.data.sources[1].id)  # this sheet has no data
    assert src_2.file_id == conn.data.sources[1].file_id
    assert updated_conn.data.sources[1].s3_filename is None
    assert await s3_file_exists(s3_client, s3_persistent_bucket, conn.data.sources[1].s3_filename) is False
    assert src_2.status == FileProcessingStatus.failed
    assert src_2.data_updated_at != data_updated_at_orig
    assert src_2.raw_schema is not None

    src_3 = updated_conn.get_file_source_by_id(conn.data.sources[2].id)  # this sheet does not exist
    assert src_3.file_id == conn.data.sources[2].file_id
    assert updated_conn.data.sources[2].s3_filename is None
    assert await s3_file_exists(s3_client, s3_persistent_bucket, conn.data.sources[2].s3_filename) is False
    assert src_3.status == FileProcessingStatus.failed
    assert src_3.data_updated_at != data_updated_at_orig
    assert src_3.raw_schema is not None

    src_4 = updated_conn.get_file_source_by_id(conn.data.sources[3].id)  # this sheet's whole spreadsheet does not exist
    assert src_4.file_id == conn.data.sources[3].file_id
    assert updated_conn.data.sources[3].s3_filename is None
    assert await s3_file_exists(s3_client, s3_persistent_bucket, conn.data.sources[3].s3_filename) is False
    assert src_4.status == FileProcessingStatus.failed
    assert src_4.data_updated_at != data_updated_at_orig
    assert src_4.raw_schema is not None

    src_5 = updated_conn.get_file_source_by_id(
        conn.data.sources[4].id
    )  # this is a valid source, but it failed during the previous update
    assert src_5.status == FileProcessingStatus.ready
    assert src_5.data_updated_at != data_updated_at_orig
    assert src_5.raw_schema is not None

    src_6 = updated_conn.get_file_source_by_id(conn.data.sources[5].id)  # no access
    assert src_6.file_id == conn.data.sources[5].file_id
    assert updated_conn.data.sources[5].s3_filename is None
    assert await s3_file_exists(s3_client, s3_persistent_bucket, conn.data.sources[5].s3_filename) is False
    assert src_6.status == FileProcessingStatus.failed
    assert src_6.data_updated_at != data_updated_at_orig
    assert src_6.raw_schema is not None

    error_registry = updated_conn.data.component_errors
    assert len(error_registry.items) == 4
    for err_pack in error_registry.items:
        assert all("request-id" in err.details for err in err_pack.errors)


@pytest.mark.asyncio
async def test_update_connection_data_without_save(
    fu_client,
    redis_cli,
    s3_tmp_bucket,
    s3_persistent_bucket,
    saved_yadocs_connection: YaDocsFileS3Connection,
    reader_app,
):
    conn = saved_yadocs_connection
    resp = await fu_client.make_request(ReqBuilder.update_conn_data(conn, save=False, file_type="yadocs"))
    assert resp.status == 200

    file_ids = [file["file_id"] for file in resp.json["files"]]
    assert len(file_ids) == 3

    for file_id in file_ids:
        resp = await fu_client.make_request(ReqBuilder.file_status(file_id))
        assert resp.status == 200
        assert resp.json["file_id"] == file_id

    await asyncio.sleep(5)

    # spreadsheet I
    resp = await fu_client.make_request(ReqBuilder.file_status(file_ids[0]))
    assert resp.status == 200
    assert resp.json["file_id"] == file_ids[0]
    assert resp.json["status"] == "ready"

    resp = await fu_client.make_request(ReqBuilder.file_sources(file_ids[0]))
    sources = resp.json["sources"]
    assert sources[0]["is_applicable"]
    assert sources[0]["error"] is None

    assert not sources[1]["is_applicable"]
    assert sources[1]["error"]["code"] == "ERR.FILE.NO_DATA"
    assert "request-id" in sources[1]["error"]["details"]

    assert not sources[2]["is_applicable"]
    assert sources[2]["error"]["code"] == "ERR.FILE.NOT_FOUND"

    assert sources[3]["is_applicable"]
    assert sources[3]["error"] is None

    # spreadsheet II
    resp = await fu_client.make_request(ReqBuilder.file_status(file_ids[1]))
    assert resp.status == 200
    assert resp.json["file_id"] == file_ids[1]
    assert resp.json["status"] == "failed"

    resp = await fu_client.make_request(ReqBuilder.file_sources(file_ids[1]))
    sources = resp.json["sources"]
    assert not sources[0]["is_applicable"]
    assert sources[0]["error"]["code"] == "ERR.FILE.NOT_FOUND"

    resp = await fu_client.make_request(ReqBuilder.source_status(file_ids[1], sources[0]["source_id"]))
    assert resp.json["file_id"] == file_ids[1]
    assert resp.json["status"] == "failed"
    assert resp.json["error"]["code"] == "ERR.FILE.NOT_FOUND"

    # spreadsheet III
    resp = await fu_client.make_request(ReqBuilder.file_status(file_ids[2]))
    assert resp.status == 200
    assert resp.json["file_id"] == file_ids[2]
    assert resp.json["status"] == "failed"

    resp = await fu_client.make_request(ReqBuilder.file_sources(file_ids[2]))
    sources = resp.json["sources"]
    assert not sources[0]["is_applicable"]
    assert sources[0]["error"]["code"] == "ERR.FILE.NOT_FOUND"

    resp = await fu_client.make_request(ReqBuilder.source_status(file_ids[2], sources[0]["source_id"]))
    assert resp.json["file_id"] == file_ids[2]
    assert resp.json["status"] == "failed"
    assert resp.json["error"]["code"] == "ERR.FILE.NOT_FOUND"


@pytest.mark.asyncio
async def test_update_in_progress_sources(
    fu_client,
    default_async_usm_per_test: AsyncUSManager,
    redis_cli,
    s3_tmp_bucket,
    s3_persistent_bucket,
    saved_yadocs_connection: YaDocsFileS3Connection,
    reader_app,
):
    usm = default_async_usm_per_test
    conn = saved_yadocs_connection

    # making the source incomplete
    conn.data.sources[0].sheet_id = None
    conn.data.sources[0].status = FileProcessingStatus.in_progress
    await usm.save(conn)

    resp = await fu_client.make_request(
        ReqBuilder.update_conn_data(
            conn,
            save=False,
            require_ok=False,
            file_type="yadocs",
        )
    )
    assert resp.status == 400

    assert resp.json["message"] == exc.CannotUpdateDataError.default_message
    assert resp.json["details"] == {
        "incomplete_sources": [{"source_id": conn.data.sources[0].id, "title": conn.data.sources[0].title}]
    }
