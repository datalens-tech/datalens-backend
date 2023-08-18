import pytest
from aiohttp.test_utils import TestClient

from bi_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite

from bi_connector_postgresql_tests.db.api.base import PostgreSQLDashSQLConnectionTest
from bi_connector_postgresql_tests.db.config import DASHSQL_QUERY


class TestPostgresDashSQL(PostgreSQLDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.mark.asyncio
    async def test_result(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=DASHSQL_QUERY,
        )

        resp_data = await resp.json()
        assert resp_data[0]['event'] == 'metadata', resp_data
        assert resp_data[0]['data']['names'] == [
            'aa', 'bb', 'cc', 'dd', 'ee', 'ff']
        assert resp_data[0]['data']['driver_types'] == [
            25, 1007, 23, 1114, 1184, 17]
        assert resp_data[0]['data']['postgresql_typnames'] == [
            'text', '_int4', 'int4', 'timestamp', 'timestamptz', 'bytea']
        assert resp_data[0]['data']['db_types'] == [
            'text', 'array(integer)', 'integer', 'timestamp', 'timestamp', 'bytea']
        assert resp_data[0]['data']['bi_types'] == [
            'string', 'array_int', 'integer', 'genericdatetime', 'genericdatetime', 'unsupported']

        assert resp_data[-1]['event'] == 'footer', resp_data[-1]

    @pytest.mark.asyncio
    async def test_result_with_error(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query='select 1/0',
            fail_ok=True,
        )

        assert resp.status == 400
        resp_data = await resp.json()
        assert resp_data['code'] == 'ERR.DS_API.DB.ZERO_DIVISION'
        assert resp_data['message'] == 'Division by zero.'
        assert resp_data['details'] == {'db_message': 'division by zero'}
