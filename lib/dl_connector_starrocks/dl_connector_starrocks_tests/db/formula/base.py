import pytest

from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from dl_connector_starrocks.formula.constants import StarRocksDialect
import dl_connector_starrocks_tests.db.config as test_config


class StarRocksTestBase(FormulaConnectorTestBase):
    supports_arrays = False
    supports_uuid = False
    dialect = StarRocksDialect.STARROCKS_3_0

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CORE_URL
