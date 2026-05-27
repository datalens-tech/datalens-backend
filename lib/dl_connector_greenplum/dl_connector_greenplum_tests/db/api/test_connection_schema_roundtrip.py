from copy import deepcopy
from typing import Any

import pytest

from dl_api_lib_testing.connector.schema_roundtrip_suite import ConnectionSchemaRoundtripTestSuite

from dl_connector_greenplum_tests.db.api.base import GP6ConnectionTestBase
from dl_connector_greenplum_tests.db.config import (
    CONNECTION_PARAMS_BY_VERSION,
    GreenplumVersion,
)

GP6_PARAMS = CONNECTION_PARAMS_BY_VERSION[GreenplumVersion.GP6]


class TestGreenplumConnectionSchemaRoundtrip(
    GP6ConnectionTestBase,
    ConnectionSchemaRoundtripTestSuite,
):
    @pytest.fixture(scope="class")
    def input_data(self) -> dict[str, Any]:
        return {
            "host": GP6_PARAMS["host"],
            "port": GP6_PARAMS["port"],
            "username": GP6_PARAMS["username"],
            "password": GP6_PARAMS["password"],
            "db_name": GP6_PARAMS["db_name"],
            "enforce_collate": "auto",
            "ssl_enable": "off",
            "raw_sql_level": "off",
            "data_export_forbidden": "off",
            "cache_ttl_sec": None,
            "cache_invalidation_throttling_interval_sec": None,
        }

    @pytest.fixture(scope="class")
    def output_data(self, input_data: dict[str, Any]) -> dict[str, Any]:
        result = deepcopy(input_data)
        del result["password"]
        return {
            **result,
            **self._common_output_data(db_type="greenplum"),
        }
