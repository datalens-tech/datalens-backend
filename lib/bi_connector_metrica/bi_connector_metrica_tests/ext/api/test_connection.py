import json
import uuid
from copy import deepcopy

import pytest

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from bi_connector_metrica_tests.ext.api.base import (
    AppMetricaConnectionTestBase,
    MetricaConnectionTestBase,
)
from bi_connector_metrica_tests.ext.config import METRIKA_SAMPLE_COUNTER_ID


class TestMetricaConnection(MetricaConnectionTestBase, DefaultConnectorConnectionTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorConnectionTestSuite.test_test_connection: "Doesn't work for Metrica",
        }
    )

    @pytest.mark.parametrize("bad_counter", ["44147844,-44147844", "44147844,asdf"])
    def test_invalid_counter(
        self, control_api_sync_client: SyncHttpClientBase, connection_params: dict, bad_counter: str
    ) -> None:
        params = deepcopy(connection_params)
        params["counter_id"] = bad_counter
        params["name"] = f"metrica_{uuid.uuid4().hex}"
        params["type"] = self.conn_type.name
        resp = control_api_sync_client.post(
            "/api/v1/connections", data=json.dumps(params), content_type="application/json"
        )
        assert resp.status_code == 400, resp.json

    def test_update_connection(
        self,
        control_api_sync_client: SyncHttpClientBase,
        saved_connection_id: str,
        metrica_token: str,
    ) -> None:
        new_counter_id = f"{METRIKA_SAMPLE_COUNTER_ID},{METRIKA_SAMPLE_COUNTER_ID}"
        update_resp = control_api_sync_client.put(
            f"/api/v1/connections/{saved_connection_id}",
            data=json.dumps({"counter_id": new_counter_id, "token": metrica_token}),
            content_type="application/json",
        )
        assert update_resp.status_code == 200, update_resp.json

        update_resp = control_api_sync_client.put(
            f"/api/v1/connections/{saved_connection_id}",
            data=json.dumps({"token": "asdf"}),
            content_type="application/json",
        )
        assert update_resp.status_code == 200, update_resp.json


class TestAppMetricaConnection(AppMetricaConnectionTestBase, DefaultConnectorConnectionTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorConnectionTestSuite.test_test_connection: "Doesn't work for AppMetrica",
        }
    )
