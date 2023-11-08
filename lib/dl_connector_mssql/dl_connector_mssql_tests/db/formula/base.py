import pytest

from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from dl_connector_mssql.formula.constants import MssqlDialect as D
from dl_connector_mssql_tests.db.config import DB_URLS


class MSSQLTestBase(FormulaConnectorTestBase):
    dialect = D.MSSQLSRV_14_0
    supports_arrays = False
    supports_uuid = True
    bool_is_expression = True

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_URLS[self.dialect]
