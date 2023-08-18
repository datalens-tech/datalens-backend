import logging
import os

import pytest
import asyncio

from bi_testing.api_wrappers import Req

from bi_file_uploader_lib.testing.data_gen import generate_sample_csv_data_str

from bi_file_uploader_api_lib_tests.req_builder import ReqBuilder


try:
    # Arcadia testing stuff
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None


LOGGER = logging.getLogger(__name__)
ARCADIA_PREFIX = 'datalens/backend/lib/bi_file_uploader_api_lib/bi_file_uploader_api_lib_tests/db/'


@pytest.fixture(scope='function', autouse=True)
async def use_local_task_processor_auto(use_local_task_processor):
    yield


@pytest.fixture(scope="session")
def csv_data():
    return f'''f1,f2,f3,Дата,Дата и время
qwe,123,45.9,2021-02-04,2021-02-04 12:00:00
asd,345,47.9,2021-02-05,2021-02-05 14:01:00
zxc,456,"49,9",2021-02-06,2021-02-06 11:59:00
zxc,456,,,2021-02-06 11:59:00
{'zxc'*35000},456,49.9,2021-02-06,
'''


@pytest.fixture(scope="session")
def excel_data():
    filename = 'data.xlsx'

    if yatest_common is not None:
        path = yatest_common.source_path(ARCADIA_PREFIX + filename)
    else:
        dirname = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(dirname, filename)

    with open(path, 'rb') as fd:
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


@pytest.fixture(scope="function")
async def uploaded_file_id(s3_tmp_bucket, fu_client, upload_file_req) -> str:
    resp = await fu_client.make_request(upload_file_req)
    assert resp.status == 201
    await asyncio.sleep(3)
    return resp.json['file_id']


@pytest.fixture(scope="function")
async def uploaded_excel_id(
        s3_tmp_bucket,
        fu_client,
        upload_excel_req,
        reader_app,
) -> str:
    resp = await fu_client.make_request(upload_excel_req)
    assert resp.status == 201
    await asyncio.sleep(3)
    return resp.json['file_id']
