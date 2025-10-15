from typing import Optional

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_greenplum_tests.db.api.base import GreenplumConnectionTestBase


class TestGreenplumSQLConnection(GreenplumConnectionTestBase, DefaultConnectorConnectionTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectorConnectionTestSuite.test_connection_sources: (
                "Fails because we use PG instead of GP: "
                'psycopg2.errors.UndefinedTable: relation "pg_partitions" does not exist'
            ),
        },
    )

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
        assert response.json["db_type"] == "greenplum"
        assert response.json["cache_ttl_sec"] is None
