import pytest

from dl_formula_testing.testcases.base import (
    FormulaConnectorTestBase
)

from bi_connector_mysql.formula.constants import MySQLDialect as D

from bi_connector_mysql_tests.db.config import DB_URLS


class MySQLTestBase(FormulaConnectorTestBase):
    supports_arrays = False
    supports_uuid = False

    @pytest.fixture(scope='class')
    def db_url(self) -> str:
        return DB_URLS[self.dialect]


class MySQL_5_6TestBase(MySQLTestBase):
    dialect = D.MYSQL_5_6


class MySQL_8_0_12TestBase(MySQLTestBase):
    dialect = D.MYSQL_8_0_12
