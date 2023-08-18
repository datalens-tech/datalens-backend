import pytest
from aiohttp.test_utils import TestClient

from bi_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite

from bi_connector_mysql_tests.db.api.base import MySQLDashSQLConnectionTest


class TestMySQLDashSQL(MySQLDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.mark.asyncio
    async def test_percent_char(self, data_api_lowlevel_aiohttp_client: TestClient, saved_connection_id: str):
        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query='select concat(0, \'%\') as value',
        )

        resp_data = await resp.json()
        assert resp_data[0]['event'] == 'metadata', resp_data
        assert resp_data[0]['data']['names'] == [
            'value',
        ]
        assert resp_data[1]['event'] == 'row'
        assert resp_data[1]['data'] == [
            '0%',
        ]
