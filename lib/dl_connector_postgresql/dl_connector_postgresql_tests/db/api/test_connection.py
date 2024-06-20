from typing import Optional

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from dl_connector_postgresql_tests.db.api.base import PostgreSQLConnectionTestBase


class TestPostgreSQLConnection(PostgreSQLConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_create_connection(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        bi_headers: Optional[dict[str, str]],
    ) -> None:
        assert saved_connection_id
        response = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}",
            headers=bi_headers,
        )
        assert response.status_code == 200
        assert response.json["db_type"] == "postgres"
        assert response.json["cache_ttl_sec"] is None
