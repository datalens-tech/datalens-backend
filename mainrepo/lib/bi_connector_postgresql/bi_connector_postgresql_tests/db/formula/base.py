import pytest

from bi_formula_testing.testcases.base import (
    FormulaConnectorTestBase
)

from bi_connector_postgresql.formula.constants import PostgreSQLDialect as D

from bi_connector_postgresql_tests.db.config import DB_URLS


class PostgreSQLTestBase(FormulaConnectorTestBase):
    supports_arrays = True
    supports_uuid = True

    @pytest.fixture(scope='class')
    def db_url(self) -> str:
        return DB_URLS[self.dialect]


class PostgreSQL_9_3TestBase(PostgreSQLTestBase):
    dialect = D.POSTGRESQL_9_3


class PostgreSQL_9_4TestBase(PostgreSQLTestBase):
    dialect = D.POSTGRESQL_9_4


class CompengTestBase(PostgreSQLTestBase):
    dialect = D.COMPENG
