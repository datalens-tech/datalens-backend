from copy import deepcopy
from typing import Any

import pytest

from dl_api_lib_testing.connector.schema_roundtrip_suite import ConnectionSchemaRoundtripTestSuite

from dl_connector_oracle.core.constants import OracleDbNameType
from dl_connector_oracle_tests.db.api.base import OracleConnectionTestBase
from dl_connector_oracle_tests.db.config import CoreConnectionSettings


class TestOracleConnectionSchemaRoundtrip(
    OracleConnectionTestBase,
    ConnectionSchemaRoundtripTestSuite,
):
    @pytest.fixture(scope="class")
    def input_data(self) -> dict[str, Any]:
        return {
            "host": CoreConnectionSettings.HOST,
            "port": CoreConnectionSettings.PORT,
            "username": CoreConnectionSettings.USERNAME,
            "password": CoreConnectionSettings.PASSWORD,
            "db_name": CoreConnectionSettings.DB_NAME,
            "db_connect_method": OracleDbNameType.service_name.name,
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
            **self._common_output_data(db_type="oracle"),
        }
