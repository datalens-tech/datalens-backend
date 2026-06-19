import pytest
import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)

PG_HASH_FUNCTION_SUPPORT = HashFunctionSupport(
    md5=True,
    sha1=True,
    sha256=True,
)


class PostgreSQLHashFunctionTestSuite(DefaultHashFunctionFormulaConnectorTestSuite):
    hash_function_support = PG_HASH_FUNCTION_SUPPORT

    @pytest.mark.usefixtures("enabled_pgcrypto_extension")
    def test_sha1(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        return super().test_sha1(dbe, data_table)

    @pytest.mark.usefixtures("enabled_pgcrypto_extension")
    def test_sha256(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        return super().test_sha256(dbe, data_table)


class TestHashFunctionPostgreSQL9p3(
    PostgreSQL9p3TestBase,
    PostgreSQLHashFunctionTestSuite,
):
    pass


class TestHashFunctionPostgreSQL9p4(
    PostgreSQL9p4TestBase,
    PostgreSQLHashFunctionTestSuite,
):
    pass
