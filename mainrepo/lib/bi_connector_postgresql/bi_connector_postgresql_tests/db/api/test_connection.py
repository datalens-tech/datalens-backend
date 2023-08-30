from bi_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from bi_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase

from bi_connector_postgresql_tests.db.api.base import PostgreSQLConnectionTestBase


class TestPostgreSQLConnection(PostgreSQLConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_create_connection(self, control_api_sync_client: SyncHttpClientBase, saved_connection_id: str) -> None:
        assert saved_connection_id
        response = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}",
        )
        assert response.status_code == 200
        assert response.json['db_type'] == 'postgres'
        assert response.json['cache_ttl_sec'] is None
