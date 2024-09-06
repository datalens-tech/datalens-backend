from typing import Optional

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite
from dl_testing.regulated_test import RegulatedTestCase

from dl_connector_bigquery_tests.ext.api.base import (
    BigQueryConnectionTestBadProjectId,
    BigQueryConnectionTestBase,
    BigQueryConnectionTestMalformedCreds,
)


class TestBigQueryConnection(BigQueryConnectionTestBase, DefaultConnectorConnectionTestSuite):
    pass


class ErrorHandlingTestBase(BigQueryConnectionTestBase, ConnectionTestBase, RegulatedTestCase):
    def test_connection_sources_error(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        bi_headers: Optional[dict[str, str]],
    ) -> None:
        resp = control_api_sync_client.get(
            url=f"/api/v1/connections/{saved_connection_id}/info/sources",
            headers=bi_headers,
        )
        assert resp.status_code == 400, resp.json
        resp_data = resp.json
        assert "code" in resp_data, resp_data
        assert "message" in resp_data, resp_data


class TestErrorHandlingMalformedCreds(BigQueryConnectionTestMalformedCreds, ErrorHandlingTestBase):
    pass


class TestErrorHandlingBadProjectId(BigQueryConnectionTestBadProjectId, ErrorHandlingTestBase):
    pass
