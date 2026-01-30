import pytest
import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_hash import (
    DefaultHashFunctionFormulaConnectorTestSuite,
    HashFunctionSupport,
)

from dl_connector_greenplum_tests.db.formula.base import GreenplumTestBase


GP_HASH_FUNCTION_SUPPORT = HashFunctionSupport(
    md5=True,
    sha1=True,
    sha256=True,
)


class GreenplumHashFunctionTestSuite(DefaultHashFunctionFormulaConnectorTestSuite):
    hash_function_support = GP_HASH_FUNCTION_SUPPORT

    @pytest.mark.usefixtures("enabled_pgcrypto_extension")
    def test_sha1(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        return super().test_sha1(dbe, data_table)

    @pytest.mark.usefixtures("enabled_pgcrypto_extension")
    def test_sha256(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        return super().test_sha256(dbe, data_table)


class TestHashFunctionGreenplum(GreenplumTestBase, GreenplumHashFunctionTestSuite):
    pass
