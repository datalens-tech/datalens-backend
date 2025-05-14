from __future__ import annotations

import logging
import os
import uuid

import aiohttp.web
import pytest
import pytest_asyncio

from dl_constants.enums import FileProcessingStatus
from dl_file_secure_reader_lib.app import create_app as create_reader_app
from dl_file_uploader_lib.enums import FileType
from dl_file_uploader_lib.redis_model.models import DataFile
from dl_file_uploader_lib.testing.data_gen import generate_sample_csv_data_str
from dl_file_uploader_task_interface.tasks import (
    ParseFileTask,
    SaveSourceTask,
)
from dl_task_processor.state import wait_task

from .utils import create_file_connection


LOGGER = logging.getLogger(__name__)


@pytest_asyncio.fixture(scope="function")
async def upload_file(s3_tmp_bucket, s3_persistent_bucket, s3_client, redis_model_manager):
    async def uploader(csv_data: bytes) -> DataFile:
        dfile = DataFile(
            manager=redis_model_manager,
            filename="test_file.csv",
            file_type=FileType.csv,
            size=len(csv_data),
            status=FileProcessingStatus.in_progress,
        )
        await s3_client.put_object(
            ACL="private",
            Bucket=s3_tmp_bucket,
            Key=dfile.s3_key_old,
            Body=csv_data,
        )

        await dfile.save()
        return dfile

    yield uploader


@pytest_asyncio.fixture(scope="function")
async def uploaded_file(upload_file) -> DataFile:
    csv_data = f"""f1,f2,f3,Дата,Дата и время
qwe,123,45.9,2021-02-04,2021-02-04 12:00:00
asd,345,47.9,2021-02-05,2021-02-05 14:01:00
zxc,456,"49,9",2021-02-06,2021-02-06 11:59:00
zxc,456,,,2021-02-06 11:59:00
{'zxc' * 35000},456,49.9,2021-02-06,""".encode(
        "utf-8"
    )
    data_file_desc = await upload_file(csv_data)
    yield data_file_desc


another_uploaded_file = uploaded_file


@pytest_asyncio.fixture(scope="function")
async def uploaded_file_id(uploaded_file) -> str:
    yield uploaded_file.id


@pytest_asyncio.fixture(scope="function")
async def another_uploaded_file_id(another_uploaded_file) -> str:
    yield another_uploaded_file.id


@pytest_asyncio.fixture(scope="function")
async def uploaded_file_dt(upload_file) -> DataFile:
    csv_data = """Date time
13.10.2023 21:02:36
13.10.2023 10:38:48
12.10.2023 20:47:22
12.10.2023 20:46:24
12.10.2023 20:44:19""".encode(
        "utf-8"
    )
    data_file_desc = await upload_file(csv_data)
    yield data_file_desc


@pytest_asyncio.fixture(scope="function")
async def uploaded_file_dt_id(uploaded_file_dt) -> str:
    yield uploaded_file_dt.id


@pytest_asyncio.fixture(scope="function")
async def uploaded_10mb_file_id(s3_tmp_bucket, s3_persistent_bucket, s3_client, redis_model_manager) -> str:
    csv_data = generate_sample_csv_data_str(row_count=10000, str_cols_count=30).encode("utf-8")

    dfile = DataFile(
        manager=redis_model_manager,
        filename="test_file_10mb.csv",
        file_type=FileType.csv,
        size=len(csv_data),
        status=FileProcessingStatus.in_progress,
    )
    await s3_client.put_object(
        ACL="private",
        Bucket=s3_tmp_bucket,
        Key=dfile.s3_key_old,
        Body=csv_data,
    )

    await dfile.save()
    yield dfile.id


@pytest_asyncio.fixture(scope="function")
async def uploaded_excel_file(s3_tmp_bucket, s3_persistent_bucket, s3_client, redis_model_manager):
    async def uploader(filename: str) -> DataFile:
        dfile = DataFile(
            manager=redis_model_manager,
            filename=filename,
            file_type=FileType.xlsx,
            status=FileProcessingStatus.in_progress,
        )

        dirname = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(dirname, "data", filename)

        with open(path, "rb") as fd:
            await s3_client.put_object(
                ACL="private",
                Bucket=s3_tmp_bucket,
                Key=dfile.s3_key_old,
                Body=fd.read(),
            )

        await dfile.save()
        return dfile

    yield uploader


@pytest_asyncio.fixture(scope="function")
async def uploaded_excel(uploaded_excel_file) -> DataFile:
    filename = "data.xlsx"
    data_file_desc = await uploaded_excel_file(filename)
    yield data_file_desc


@pytest_asyncio.fixture(scope="function")
async def uploaded_excel_id(uploaded_excel) -> str:
    yield uploaded_excel.id


@pytest_asyncio.fixture(scope="function")
async def uploaded_excel_with_one_row(uploaded_excel_file) -> DataFile:
    filename = "one_row_table.xlsx"
    data_file_desc = await uploaded_excel_file(filename)
    yield data_file_desc


@pytest_asyncio.fixture(scope="function")
async def uploaded_excel_with_one_row_id(uploaded_excel_with_one_row) -> str:
    yield uploaded_excel_with_one_row.id


@pytest_asyncio.fixture(scope="function")
async def uploaded_excel_no_header(uploaded_excel_file) -> DataFile:
    filename = "no_header.xlsx"
    data_file_desc = await uploaded_excel_file(filename)
    yield data_file_desc


@pytest_asyncio.fixture(scope="function")
async def uploaded_excel_no_header_id(uploaded_excel_no_header) -> str:
    yield uploaded_excel_no_header.id


@pytest_asyncio.fixture(scope="function")
async def uploaded_invalid_excel(uploaded_excel_file) -> DataFile:
    filename = "invalid_excel.xlsx"
    data_file_desc = await uploaded_excel_file(filename)
    yield data_file_desc


@pytest_asyncio.fixture(scope="function")
async def uploaded_invalid_excel_id(uploaded_invalid_excel) -> str:
    yield uploaded_invalid_excel.id


@pytest.fixture(scope="function")
def reader_app(loop, secure_reader):
    current_app = create_reader_app()
    runner = aiohttp.web.AppRunner(current_app)
    loop.run_until_complete(runner.setup())
    site = aiohttp.web.UnixSite(runner, path=secure_reader.SOCKET)
    return loop.run_until_complete(site.start())


@pytest.fixture(scope="session")
def tenant_id() -> str:
    return uuid.uuid4().hex


@pytest.fixture(scope="function")
async def saved_file_connection_id(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    uploaded_file_id,
    default_async_usm_per_test,
    tenant_id,
) -> str:
    task_parse = await task_processor_client.schedule(
        ParseFileTask(
            tenant_id=tenant_id,
            file_id=uploaded_file_id,
        )
    )
    result = await wait_task(task_parse, task_state)
    assert result[-1] == "success"

    df = await DataFile.get(manager=redis_model_manager, obj_id=uploaded_file_id)
    source = df.sources[0]
    assert source.status == FileProcessingStatus.ready

    conn = await create_file_connection(default_async_usm_per_test, df.id, source.id, source.raw_schema)

    task_save = await task_processor_client.schedule(
        SaveSourceTask(
            tenant_id=tenant_id,
            file_id=uploaded_file_id,
            src_source_id=source.id,
            dst_source_id=source.id,
            connection_id=conn.uuid,
        )
    )
    await wait_task(task_save, task_state)

    return conn.uuid
