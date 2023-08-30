import abc
from typing import Any, Optional

from aiohttp.test_utils import ClientResponse, TestClient

from bi_api_lib_testing.connection_base import ConnectionTestBase
from bi_api_lib_testing.data_api_base import DataApiTestBase


class DashSQLTestBase(ConnectionTestBase, DataApiTestBase, metaclass=abc.ABCMeta):
    async def get_dashsql_response(
            self, data_api_aio: TestClient,
            conn_id: str,
            query: str,
            fail_ok: bool = False,
            params: Optional[dict] = None,
            connector_specific_params: Optional[dict] = None,
    ) -> ClientResponse:
        request_body: dict[str, Any] = {'sql_query': query}
        if params:
            request_body['params'] = params
        if connector_specific_params:
            request_body['connector_specific_params'] = connector_specific_params

        resp = await data_api_aio.post(f'api/v1/connections/{conn_id}/dashsql', json=request_body)
        resp_data = await resp.json()
        if not fail_ok:
            assert resp.status == 200, resp_data
        return resp
