import json

from bi_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from bi_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from bi_connector_bundle_ch_filtered_ya_cloud_tests.db.ch_geo_filtered.api.base import CHGeoFilteredConnectionTestBase


class TestCHGeoFilteredSQLConnection(CHGeoFilteredConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_get_update_delete_ch_geo_filtered_connection(
            self, control_api_sync_client: SyncHttpClientBase,
            saved_connection_id: str
    ):
        client = control_api_sync_client
        conn_id = saved_connection_id

        conn_get_resp = client.get(f'/api/v1/connections/{conn_id}')
        assert conn_get_resp.status_code == 200
        conn = conn_get_resp.json
        assert conn['db_type'] == 'ch_geo_filtered'

        conn_data = {
            'host': 'localhost',
            'port': 8443,
            'username': 'root',
            'mdb_cluster_id': 'some_cluster_id',
            'mdb_folder_id': 'some_folder_id',
            'allowed_tables': ['asdf', 'qwer'],
            'mp_producrt_id': 'zxcv',
            'password': 'qwerty',
            'secure': 'on',
            'db_name': 'test_db',
            'cache_ttl_sec': None,
            'data_export_forbidden': True,
            'endpoint': 'jkl;',
        }
        update_resp = client.put(
            f'/api/v1/connections/{conn_id}',
            data=json.dumps(conn_data),
            content_type='application/json'
        )
        assert update_resp.status_code == 400, update_resp.json
        unknown_fields = set(
            field_name for field_name, resp_value in update_resp.json.items() if resp_value == ['Unknown field.']
        )
        assert unknown_fields == set(conn_data.keys())

        assert (delete_resp := client.delete(f'/api/v1/connections/{conn_id}')).status_code == 200, delete_resp.json
        assert (conn_get_resp := client.get(f'/api/v1/connections/{conn_id}')).status_code == 404, conn_get_resp.json
