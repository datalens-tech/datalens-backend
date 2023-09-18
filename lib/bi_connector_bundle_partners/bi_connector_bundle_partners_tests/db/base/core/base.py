from __future__ import annotations

from typing import (
    Generic,
    TypeVar,
)

import pytest

from dl_core_testing.testcases.connection import BaseConnectionTestClass

from bi_connector_bundle_partners.base.core.us_connection import PartnersCHConnectionBase
import bi_connector_bundle_partners_tests.db.config as test_config

_CONN_TV = TypeVar("_CONN_TV", bound=PartnersCHConnectionBase)


class BasePartnersClass(BaseConnectionTestClass, Generic[_CONN_TV]):
    core_test_config = test_config.CORE_TEST_CONFIG

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_URL

    @pytest.fixture(scope="function")
    def connection_creation_params(self) -> dict:
        return dict(
            db_name=test_config.CoreConnectionSettings.DB_NAME,
            endpoint=test_config.CoreConnectionSettings.ENDPOINT,
            cluster_name=test_config.CoreConnectionSettings.CLUSTER_NAME,
            max_execution_time=test_config.CoreConnectionSettings.MAX_EXECUTION_TIME,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )
