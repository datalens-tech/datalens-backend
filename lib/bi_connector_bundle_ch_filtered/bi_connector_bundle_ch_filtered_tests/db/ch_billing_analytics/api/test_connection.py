import json

from bi_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from bi_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.api.base import CHBillingAnalyticsConnectionTestBase


class TestCHBillingAnalyticsSQLConnection(CHBillingAnalyticsConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_update_connection(
            self, control_api_sync_client: SyncHttpClientBase,
            saved_connection_id: str,
    ):
        client = control_api_sync_client
        conn_id = saved_connection_id

        conn_data = {
            'host': 'localhost',
            'port': 8443,
            'username': 'root',
            'mdb_cluster_id': 'some_cluster_id',
            'mdb_folder_id': 'some_folder_id',
            'password': 'qwerty',
            'secure': 'on',
            'db_name': 'test_db',
            'cache_ttl_sec': None,
        }

        update_resp = client.put(
            '/api/v1/connections/{}'.format(conn_id),
            data=json.dumps(conn_data),
            content_type='application/json'
        )

        assert update_resp.status_code == 400, update_resp.json
        unknown_fields = set(
            field_name for field_name, resp_value in update_resp.json.items() if resp_value == ['Unknown field.'])

        assert unknown_fields == set(conn_data.keys())
