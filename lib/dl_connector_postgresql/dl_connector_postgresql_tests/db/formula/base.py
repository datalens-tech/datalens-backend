import pytest

from dl_formula_testing.database import FormulaDbConfig
from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D
from dl_connector_postgresql_tests.db.config import DB_URLS


class PostgreSQLTestBase(FormulaConnectorTestBase):
    supports_arrays = True
    supports_uuid = True

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_URLS[self.dialect]

    @pytest.fixture(scope="function")
    def enabled_pgcrypto_extension(self, db_config: FormulaDbConfig) -> None:
        db = self.db_dispenser.get_database(db_config)
        db.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
        yield
        db.execute("DROP EXTENSION IF EXISTS pgcrypto;")


class PostgreSQL_9_3TestBase(PostgreSQLTestBase):
    dialect = D.POSTGRESQL_9_3


class PostgreSQL_9_4TestBase(PostgreSQLTestBase):
    dialect = D.POSTGRESQL_9_4


class CompengTestBase(PostgreSQLTestBase):
    dialect = D.COMPENG
