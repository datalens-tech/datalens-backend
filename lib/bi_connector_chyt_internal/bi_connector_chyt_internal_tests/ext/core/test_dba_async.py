from __future__ import annotations

import random

import attr
import pytest
import requests
import shortuuid
from aiohttp import web
from aiohttp.test_utils import TestClient
from multidict import CIMultiDict

from bi_api_commons.base_models import RequestContextInfo
from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from bi_connector_chyt_internal.core.async_adapters import AsyncCHYTInternalAdapter, AsyncCHYTUserAuthAdapter
from bi_connector_chyt_internal.core.target_dto import CHYTInternalConnTargetDTO, CHYTUserAuthConnTargetDTO


@attr.s(auto_attribs=True)
class MirrorCliqueMock:
    @attr.s(auto_attribs=True)
    class ReqData:
        headers: CIMultiDict[str, str]
        body: bytes

    requests: dict[str, ReqData]
    client: TestClient


class BaseTestAsyncCHYTAdapter:
    @pytest.fixture(scope='class')
    def yt_cluster_name(self) -> str:
        return 'hahn'

    @pytest.fixture(scope='class')
    def chyt_host(self, yt_cluster_name):
        cluster_name = yt_cluster_name
        return random.choice(requests.get(f'http://{cluster_name}.yt.yandex.net/hosts').json())

    @pytest.fixture(scope='function')
    async def mirror_clique_mock(self, aiohttp_client) -> MirrorCliqueMock:
        app = web.Application()
        req_stat = {}

        async def query_view(request: web.Request):
            body = await request.read()
            req_id = request.headers['x-request-id']
            req_stat[req_id] = MirrorCliqueMock.ReqData(
                headers=CIMultiDict(request.headers.items()),
                body=body
            )
            return web.Response(status=200)

        app.router.add_route('*', '/query', query_view)

        test_client = await aiohttp_client(app)

        return MirrorCliqueMock(
            client=test_client,
            requests=req_stat
        )

    async def _test_mirroring_simple(self, adapter_cls, conn_target_dto_w_mirroring, mirror_clique_mock: MirrorCliqueMock):
        rci = attr.evolve(
            RequestContextInfo.create_empty(),
            request_id=shortuuid.uuid(),
        )
        dba = adapter_cls(
            req_ctx_info=rci,
            target_dto=conn_target_dto_w_mirroring,
            default_chunk_size=10,
        )

        await dba.execute(DBAdapterQuery("SELECT 1"))

        assert rci.request_id in mirror_clique_mock.requests
        req_data = mirror_clique_mock.requests[rci.request_id]

        assert req_data.body == b'SELECT 1\nFORMAT JSONCompact'


class TestAsyncCHYTAdapter(BaseTestAsyncCHYTAdapter):
    @pytest.fixture(scope='class')
    def conn_target_dto(self, yt_cluster_name, chyt_host, yt_token):
        clique_alias = '*chyt_datalens_back'

        return CHYTInternalConnTargetDTO(
            conn_id=None,
            protocol='http',
            host=chyt_host,
            yt_cluster=yt_cluster_name,
            port=80,
            db_name=clique_alias,
            username='default',
            password=yt_token,
            #
            cluster_name=None,
            endpoint='query',
            connect_timeout=None,
            max_execution_time=None,
            total_timeout=None,
            mirroring_conn_target_dto=None,
            insert_quorum=None,
            insert_quorum_timeout=None,
            disable_value_processing=False,
        )

    @pytest.fixture(scope='function')
    def conn_target_dto_w_mirroring(self, yt_cluster_name, conn_target_dto, mirror_clique_mock: MirrorCliqueMock):
        return attr.evolve(
            conn_target_dto,
            mirroring_conn_target_dto=CHYTInternalConnTargetDTO(
                conn_id=None,
                protocol='http',
                host=mirror_clique_mock.client.host,
                port=mirror_clique_mock.client.port,
                yt_cluster=yt_cluster_name,
                db_name='*mirroring_clique_alias',
                username='default',
                password='some_password',
                #
                cluster_name=None,
                endpoint='query',
                connect_timeout=None,
                max_execution_time=None,
                total_timeout=None,
                mirroring_conn_target_dto=None,
                insert_quorum=None,
                insert_quorum_timeout=None,
                disable_value_processing=False,
            ),
        )

    @pytest.mark.asyncio
    async def test_mirroring_simple(self, conn_target_dto_w_mirroring, mirror_clique_mock: MirrorCliqueMock):
        await self._test_mirroring_simple(AsyncCHYTInternalAdapter, conn_target_dto_w_mirroring, mirror_clique_mock)


class TestAsyncCHYTUserAuthAdapter(BaseTestAsyncCHYTAdapter):
    @pytest.fixture(scope='class')
    def conn_target_dto(self, yt_cluster_name, chyt_host, yt_token):
        clique_alias = '*chyt_datalens_back'

        return CHYTUserAuthConnTargetDTO(
            conn_id=None,
            protocol='http',
            host=chyt_host,
            port=80,
            yt_cluster=yt_cluster_name,
            db_name=clique_alias,
            username=None,
            password=None,
            header_authorization='OAuth {}'.format(yt_token),
            header_cookie=None,
            header_csrf_token=None,
            #
            cluster_name=None,
            endpoint='query',
            connect_timeout=None,
            max_execution_time=None,
            total_timeout=None,
            mirroring_conn_target_dto=None,
            insert_quorum=None,
            insert_quorum_timeout=None,
            disable_value_processing=False,
        )

    @pytest.fixture(scope='function')
    def conn_target_dto_w_mirroring(self, conn_target_dto, mirror_clique_mock: MirrorCliqueMock, yt_token):
        return attr.evolve(
            conn_target_dto,
            mirroring_conn_target_dto=CHYTUserAuthConnTargetDTO(
                conn_id=None,
                protocol='http',
                host=mirror_clique_mock.client.host,
                port=mirror_clique_mock.client.port,
                yt_cluster=conn_target_dto.yt_cluster,
                db_name='*mirroring_clique_alias',
                username=None,
                password=None,
                header_authorization='OAuth {}'.format(yt_token),
                header_cookie=None,
                header_csrf_token=None,
                #
                cluster_name=None,
                endpoint='query',
                connect_timeout=None,
                max_execution_time=None,
                total_timeout=None,
                mirroring_conn_target_dto=None,
                insert_quorum=None,
                insert_quorum_timeout=None,
                disable_value_processing=False,
            ),
        )

    @pytest.mark.asyncio
    async def test_mirroring_simple(self, conn_target_dto_w_mirroring, mirror_clique_mock: MirrorCliqueMock):
        await self._test_mirroring_simple(AsyncCHYTUserAuthAdapter, conn_target_dto_w_mirroring, mirror_clique_mock)
