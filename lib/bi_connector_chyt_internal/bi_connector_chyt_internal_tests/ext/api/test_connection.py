import json
import uuid

import pytest

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite

from bi_connector_chyt_internal_tests.ext.api.base import (
    CHYTConnectionTestBase,
    CHYTUserAuthConnectionTestBase,
)


class TestCHYTConnection(CHYTConnectionTestBase, DefaultConnectorConnectionTestSuite):
    @pytest.mark.parametrize("bad_alias", ["*definitely_nonexistent_test_clique", "*ch_public"])
    def test_clique_alias(
        self,
        control_api_sync_client: SyncHttpClientBase,
        connection_params: dict,
        bad_alias: str,
    ) -> None:
        params = dict(
            name=f"{self.conn_type.name} conn {uuid.uuid4()}",
            type=self.conn_type.name,
            **connection_params,
        )
        resp = control_api_sync_client.post(
            "/api/v1/connections/test_connection_params",
            content_type="application/json",
            data=json.dumps(params),
        )
        assert resp.status_code == 200, resp.json

        params["alias"] = bad_alias
        resp = control_api_sync_client.post(
            "/api/v1/connections/test_connection_params",
            content_type="application/json",
            data=json.dumps(params),
        )
        assert resp.status_code == 400, resp.json


class TestCHYTUserAuthConnection(CHYTUserAuthConnectionTestBase, DefaultConnectorConnectionTestSuite):
    def test_test_connection(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        auth_headers: dict[str, str],
    ) -> None:
        resp = control_api_sync_client.post(
            f"/api/v1/connections/test_connection/{saved_connection_id}",
            content_type="application/json",
            data=json.dumps({}),
            headers=auth_headers,
        )
        assert resp.status_code == 200, resp.json

    @pytest.mark.parametrize("bad_alias", ["*definitely_nonexistent_test_clique", "*ch_public"])
    def test_clique_alias(
        self,
        control_api_sync_client: SyncHttpClientBase,
        connection_params: dict,
        auth_headers: dict[str, str],
        bad_alias: str,
    ) -> None:
        params = dict(
            name=f"{self.conn_type.name} conn {uuid.uuid4()}",
            type=self.conn_type.name,
            **connection_params,
        )
        resp = control_api_sync_client.post(
            "/api/v1/connections/test_connection_params",
            content_type="application/json",
            data=json.dumps(params),
            headers=auth_headers,
        )
        assert resp.status_code == 200, resp.json

        params["alias"] = bad_alias
        resp = control_api_sync_client.post(
            "/api/v1/connections/test_connection_params",
            content_type="application/json",
            data=json.dumps(params),
            headers=auth_headers,
        )
        assert resp.status_code == 400, resp.json
