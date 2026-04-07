import pytest

from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from dl_connector_starrocks.formula.constants import StarRocksDialect as D
from dl_connector_starrocks_tests.db.config import DB_CORE_URL


class StarRocksTestBase(FormulaConnectorTestBase):
    supports_arrays = False
    supports_uuid = False
    dialect = D.STARROCKS_3_2

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_CORE_URL
