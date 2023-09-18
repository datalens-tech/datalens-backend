import json

import shortuuid

from dl_constants.enums import ConnectionType

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase

from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from bi_connector_bundle_ch_filtered.base.core.settings import CHFrozenConnectorSettings
from bi_connector_bundle_ch_frozen_tests.db.api.base import CHFrozenConnectionTestBase


class TestCHFrozenConnection(CHFrozenConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_sources(
            self,
            control_api_sync_client: SyncHttpClientBase,
            saved_connection_id: str,
            connectors_settings: dict[ConnectionType, CHFrozenConnectorSettings],
    ) -> None:
        sources_resp = control_api_sync_client.get(f'/api/v1/connections/{saved_connection_id}/info/sources')
        assert sources_resp.status_code == 200, sources_resp.json
        all_tables = [s['parameters']['table_name'] for s in sources_resp.json['sources']]

        # all connectors settings are the same, so just pick from a random one
        connector_settings = list(connectors_settings.values())[0]
        allowed_tables = connector_settings.ALLOWED_TABLES
        subselect_tables = connector_settings.SUBSELECT_TEMPLATES
        expected_tables = (allowed_tables + [subselect_template['title'] for subselect_template in subselect_tables])
        assert all_tables == expected_tables

    def test_crud(
            self,
            control_api_sync_client: SyncHttpClientBase,
            saved_connection_id: str,
            _conn_type: ConnectionType,
    ) -> None:
        conn_type = _conn_type
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
            'raw_sql_level': 'dashsql',
        }

        # CREATE (with data)
        create_resp = control_api_sync_client.post(
            '/api/v1/connections',
            data=json.dumps({
                'name': f'{conn_type.name}_test_conn_{shortuuid.uuid()}',
                'type': conn_type.name,
                **conn_data,
            }),
            content_type='application/json'
        )
        assert create_resp.status_code == 400, create_resp.json
        unknown_fields = set(
            field_name for field_name, resp_value in create_resp.json.items() if resp_value == ['Unknown field.'])
        assert unknown_fields == set(conn_data.keys())

        # CREATE (with no data)
        create_resp = control_api_sync_client.post(
            '/api/v1/connections',
            data=json.dumps({
                'name': f'{conn_type.name}_test_conn_{shortuuid.uuid()}',
                'type': conn_type.name,
            }),
            content_type='application/json'
        )
        assert create_resp.status_code == 200
        conn_id = create_resp.json['id']

        # READ
        conn_get_resp = control_api_sync_client.get(f'/api/v1/connections/{conn_id}')
        assert conn_get_resp.status_code == 200
        conn = conn_get_resp.json
        assert conn['db_type'] == conn_type.name
        assert 'raw_sql_level' in conn

        # UPDATE
        update_resp = control_api_sync_client.put(
            f'/api/v1/connections/{conn_id}',
            data=json.dumps(conn_data),
            content_type='application/json'
        )
        assert update_resp.status_code == 400, update_resp.json
        unknown_fields = set(
            field_name for field_name, resp_value in update_resp.json.items() if resp_value == ['Unknown field.'])
        assert unknown_fields == set(conn_data.keys())

        # DELETE
        assert (delete_resp := control_api_sync_client.delete(
            f'/api/v1/connections/{conn_id}'
        )).status_code == 200, delete_resp.json
        assert (conn_get_resp := control_api_sync_client.get(
            f'/api/v1/connections/{conn_id}'
        )).status_code == 404, conn_get_resp.json
