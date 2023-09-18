from __future__ import annotations

import asyncio
import contextlib
import socket
import logging
from http import HTTPStatus

import aiohttp
import pytest

from dl_constants.enums import FieldRole

from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_testing.utils import guids_from_titles

from bi_legacy_test_bundle_tests.api_lib.utils import get_result_schema


LOGGER = logging.getLogger(__name__)


@aiohttp.web.middleware
async def log_excs_mw(request, handler):
    try:
        return await handler(request)
    except Exception as exc:
        LOGGER.exception("Handler error: %r", exc)
        raise


@contextlib.asynccontextmanager
async def run_http_server(handler, methods=('POST',), bind='127.0.0.1', sock_af=socket.AF_INET, port=0):
    """ Run a simple HTTP server on a random port with the specified aiohttp.web handler """
    app = aiohttp.web.Application(middlewares=[log_excs_mw])
    for method in methods:
        app.router.add_route(method, '/', handler)
    sock = socket.socket(sock_af, socket.SOCK_STREAM)
    try:
        sock.bind((bind, port))
        port = sock.getsockname()[1]
        server_task = asyncio.create_task(
            aiohttp.web._run_app(
                app=app,
                sock=sock,
                print=None,
            ))
        try:
            yield port
        finally:
            server_task.cancel()
    finally:
        sock.close()


class BaseTestWithFakeCH:

    _bind = '127.0.0.1'

    # Convenience renames

    @pytest.fixture
    def conn_id(self, dynamic_ch_connection_id):
        return dynamic_ch_connection_id

    @pytest.fixture
    def ds_id(self, dynamic_ch_dataset_id):
        return dynamic_ch_dataset_id

    @pytest.fixture
    def data_api_aio(self, async_api_local_env_low_level_client):
        return async_api_local_env_low_level_client

    @pytest.fixture
    def usm(self, default_async_usm_per_test):
        return default_async_usm_per_test

    # Work

    @pytest.fixture
    async def fake_server_port(self):
        async with run_http_server(self.handler, bind=self._bind) as port:
            yield port

    @pytest.fixture
    async def conn_over_fake(self, conn_id, fake_server_port, usm):
        conn = await usm.get_by_id(conn_id)
        conn.data.host = self._bind
        conn.data.port = fake_server_port
        self._update_conn(conn)
        await usm.save(conn)

    @pytest.fixture
    def req_data(self, ds_id, client):
        result_schema = get_result_schema(client, ds_id)
        return {
            "columns": guids_from_titles(result_schema, ['Discount', 'City']),
            "where": [],
        }

    @pytest.fixture
    def deps_for_req(self, data_api_aio, ds_id, req_data):
        return dict(data_api_aio=data_api_aio, ds_id=ds_id, req_data=req_data)

    async def req(self, data_api_aio, ds_id, req_data):
        resp = await data_api_aio.request(
            'post', f'/api/v1/datasets/{ds_id}/versions/draft/result',
            json=req_data)
        resp_data = await resp.json()
        return resp, resp_data

    @pytest.mark.asyncio
    async def test_main(self, deps_for_req, conn_over_fake):
        resp, resp_data = await self.req(**deps_for_req)
        await self.main_test_resp_handle(resp=resp, resp_data=resp_data)

    # Customization points

    async def handler(self, request):
        return aiohttp.web.Response(body=b"")

    def _update_conn(self, conn):
        return conn

    async def main_test_resp_handle(self, resp, resp_data):
        pass


class TestNormalCH(BaseTestWithFakeCH):

    @pytest.fixture
    async def conn_over_fake(self, conn_id, fake_server_port, usm):
        # Don't touch it for this test
        pass

    async def main_test_resp_handle(self, resp, resp_data):
        assert resp.status == 200, (resp.status, resp_data)

    async def handler(self, request):
        raise Exception("Not applicable here")


VALID_BODY = (
    '{\n\t'
    '"meta":\n\t[\n\t\t'
    '{\n\t\t\t"name": "a_1",\n\t\t\t"type": "UInt8"\n\t\t},\n\t\t'
    '{\n\t\t\t"name": "a_2",\n\t\t\t"type": "UInt8"\n\t\t}'
    '\n\t],\n\n\t'
    '"data":\n\t[\n\t\t'
    '[1, 2]'
    '\n\t],\n\n\t'
    '"rows": 1,\n\n\t'
    '"statistics":\n\t{\n\t\t"elapsed": 0.000137277,\n\t\t"rows_read": 1,\n\t\t"bytes_read": 1'
    '\n\t}\n}\n'
)


class TestFakeOkayCH(BaseTestWithFakeCH):

    async def main_test_resp_handle(self, resp, resp_data):
        assert resp.status == 200, (resp.status, resp_data)
        assert resp_data['result']['data']['Data'] == [['1.0', '2']]

    async def handler(self, request):
        return aiohttp.web.Response(body=VALID_BODY.encode('utf-8'))


class TestInvalidCHResponse(BaseTestWithFakeCH):

    async def handler(self, request):
        return aiohttp.web.Response(body=b'{}')

    async def main_test_resp_handle(self, resp, resp_data):
        assert resp.status == 400, (resp.status, resp_data)
        assert resp_data['code'] == 'ERR.DS_API.DB.SOURCE_ERROR.INVALID_RESPONSE'


class BaseTestTimeoutCH(BaseTestWithFakeCH):

    _max_execution_time = 1
    _sleep_time = 30

    def _update_conn(self, conn):
        conn = super()._update_conn(conn)
        # To avoid making the test wait for too long.
        conn.data.max_execution_time = self._max_execution_time
        return conn


class TestBodyTimeoutCH(BaseTestTimeoutCH):

    async def handler(self, request):
        # See also:
        # https://github.com/aio-libs/aiohttp/blob/e01abebc216b899a451deb60c2d7fd4c6d86f44a/tests/test_client_functional.py
        resp = aiohttp.web.StreamResponse(headers={"content-length": "100"})
        await resp.prepare(request)
        await asyncio.sleep(self._sleep_time)
        return resp

    async def main_test_resp_handle(self, resp, resp_data):
        assert resp.status == 400, (resp.status, resp_data)
        assert resp_data['code'] == 'ERR.DS_API.DB.SOURCE_ERROR.TIMEOUT'


class TestHeadTimeoutCH(BaseTestTimeoutCH):

    async def handler(self, request):
        await asyncio.sleep(self._sleep_time)
        return aiohttp.web.Response(body=VALID_BODY.encode('utf-8'))

    async def main_test_resp_handle(self, resp, resp_data):
        assert resp.status == 400, (resp.status, resp_data)
        assert resp_data['code'] == 'ERR.DS_API.DB.SOURCE_ERROR.TIMEOUT'


class TestServerDisconnectedCH(BaseTestWithFakeCH):

    async def handler(self, request):
        request.transport.close()
        return aiohttp.web.Response()

    async def main_test_resp_handle(self, resp, resp_data):
        assert resp.status == 400, (resp.status, resp_data)
        assert resp_data['code'] == 'ERR.DS_API.DB.SOURCE_ERROR.CLOSED_PREMATURELY'


# Side note: can't easily test RST (ECONNRESET) (which is mapped to CLOSED_PREMATURELY)
# on a live server; could, at most, potentially test it with monkeypatching.


class TestPayloadErrorCH(BaseTestWithFakeCH):

    async def handler(self, request):
        resp = aiohttp.web.Response(text="text")
        resp.headers["Content-Encoding"] = "gzip"
        return resp

    async def main_test_resp_handle(self, resp, resp_data):
        assert resp.status == 400, (resp.status, resp_data)
        assert resp_data['code'] == 'ERR.DS_API.DB.SOURCE_ERROR.INVALID_RESPONSE'


def test_field_from_nonexistent_avatar(api_v1, data_api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    ds.result_schema['Invalid Field'] = ds.field(avatar_id='nonexistent', source='Something')
    result_resp = data_api_v1.get_result(
        dataset=ds, group_by=[], order_by=[],
        fields=[
            ds.find_field(title='Order Date'),
            ds.find_field(title='Invalid Field'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST
    assert result_resp.bi_status_code == 'ERR.DS_API.AVATAR.NOT_FOUND.FIELD_REF'


def test_uneven_block_column_count(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    result_resp = data_api_v2.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Category').as_req_legend_item(legend_item_id=0),
            ds.find_field(title='Order Date').as_req_legend_item(legend_item_id=1, block_id=0),
            ds.find_field(title='Sales Sum').as_req_legend_item(legend_item_id=2, block_id=0),
            ds.find_field(title='Sales Sum').as_req_legend_item(legend_item_id=3, role=FieldRole.total, block_id=1),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST
    assert result_resp.bi_status_code == 'ERR.DS_API.BLOCK.UNEVEN_COLUMN_COUNT'
