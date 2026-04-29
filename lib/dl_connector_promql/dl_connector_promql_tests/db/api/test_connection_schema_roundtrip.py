from copy import deepcopy
from typing import Any

import pytest

from dl_api_lib_testing.connector.schema_roundtrip_suite import ConnectionSchemaRoundtripTestSuite

from dl_connector_promql_tests.db.api.base import PromQLConnectionTestBase
from dl_connector_promql_tests.db.config import API_CONNECTION_SETTINGS


class TestPromQLConnectionSchemaRoundtrip(
    PromQLConnectionTestBase,
    ConnectionSchemaRoundtripTestSuite,
):
    @pytest.fixture(scope="class")
    def input_data(self) -> dict[str, Any]:
        return {
            "host": API_CONNECTION_SETTINGS["host"],
            "port": API_CONNECTION_SETTINGS["port"],
            "username": API_CONNECTION_SETTINGS["username"],
            "password": API_CONNECTION_SETTINGS["password"],
            "auth_type": "password",
            "auth_header": None,
            "secure": False,
            "path": None,
            "db_name": None,
            "data_export_forbidden": "off",
            "cache_ttl_sec": None,
            "cache_invalidation_throttling_interval_sec": None,
        }

    @pytest.fixture(scope="class")
    def output_data(self, input_data: dict[str, Any]) -> dict[str, Any]:
        result = deepcopy(input_data)
        del result["password"]
        del result["auth_header"]
        return {
            **result,
            **self._common_output_data(db_type="promql", allow_dashsql_usage=True),
        }
