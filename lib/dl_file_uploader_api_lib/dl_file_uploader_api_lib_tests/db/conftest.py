import asyncio
import logging
import os

import pytest
import pytest_asyncio

from dl_api_commons.client.common import Req
from dl_file_uploader_api_lib_tests.req_builder import ReqBuilder
from dl_file_uploader_lib.testing.data_gen import generate_sample_csv_data_str
from dl_s3.utils import upload_to_s3_by_presigned


LOGGER = logging.getLogger(__name__)


@pytest_asyncio.fixture(scope="function", autouse=True)
async def use_local_task_processor_auto(use_local_task_processor):
    yield


@pytest.fixture(scope="session")
def csv_data():
    return f"""f1,f2,f3,Дата,Дата и время
qwe,123,45.9,2021-02-04,2021-02-04 12:00:00
asd,345,47.9,2021-02-05,2021-02-05 14:01:00
zxc,456,"49,9",2021-02-06,2021-02-06 11:59:00
zxc,456,,,2021-02-06 11:59:00
{'zxc'*35000},456,49.9,2021-02-06,
"""


@pytest.fixture(scope="session")
def excel_data():
    filename = "data.xlsx"

    dirname = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(dirname, filename)

    with open(path, "rb") as fd:
        return fd.read()


@pytest.fixture(scope="session")
def upload_file_req(csv_data) -> Req:
    return ReqBuilder.upload_csv(csv_data)


@pytest.fixture(scope="session")
def upload_excel_req(excel_data) -> Req:
    return ReqBuilder.upload_xlsx(excel_data)


@pytest.fixture(scope="session")
def upload_file_req_10mb() -> Req:
    csv_data = generate_sample_csv_data_str(row_count=10000, str_cols_count=30)
    LOGGER.info(f'Generated csv data sample of {len(csv_data.encode("utf-8"))} bytes')
    return ReqBuilder.upload_csv(csv_data)


@pytest.fixture(scope="session")
def upload_file_req_12mb() -> Req:
    csv_data = generate_sample_csv_data_str(row_count=12000, str_cols_count=30)
    LOGGER.info(f'Generated csv data sample of {len(csv_data.encode("utf-8"))} bytes')
    return ReqBuilder.upload_csv(csv_data)


@pytest_asyncio.fixture(scope="function")
async def uploaded_file_id(s3_tmp_bucket, s3_persistent_bucket, fu_client, csv_data) -> str:
    presigned_url_resp = await fu_client.make_request(ReqBuilder.presigned_url())
    assert presigned_url_resp.status == 200, presigned_url_resp.json

    upload_resp = await upload_to_s3_by_presigned(presigned_url_resp.json, csv_data)
    assert upload_resp.status == 204

    download_resp = await fu_client.make_request(
        ReqBuilder.presigned_url_download(presigned_url_resp.json["fields"]["key"], "csv_data.csv")
    )
    assert download_resp.status == 201, download_resp.json

    assert download_resp.status == 201
    await asyncio.sleep(3)
    return download_resp.json["file_id"]


@pytest_asyncio.fixture(scope="function")
async def uploaded_excel_id(
    s3_tmp_bucket,
    fu_client,
    excel_data,
    reader_app,
) -> str:
    presigned_url_resp = await fu_client.make_request(ReqBuilder.presigned_url())
    assert presigned_url_resp.status == 200, presigned_url_resp.json

    upload_resp = await upload_to_s3_by_presigned(presigned_url_resp.json, excel_data)
    assert upload_resp.status == 204

    download_resp = await fu_client.make_request(
        ReqBuilder.presigned_url_download(presigned_url_resp.json["fields"]["key"], "data.xlsx")
    )
    assert download_resp.status == 201, download_resp.json

    assert download_resp.status == 201
    await asyncio.sleep(3)
    return download_resp.json["file_id"]
