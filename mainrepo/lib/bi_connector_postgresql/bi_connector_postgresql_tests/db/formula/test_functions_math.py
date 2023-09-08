import pytest

from bi_formula_testing.testcases.functions_math import (
    DefaultMathFunctionFormulaConnectorTestSuite,
)
from bi_formula_testing.evaluator import DbEvaluator
from bi_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase, PostgreSQL_9_4TestBase,
)


class PostgreSQLMatchTestMixin:
    def test_log_data_type_compatibility(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('LOG(DB_CAST(4.0, "numeric", 10, 5), DB_CAST(2.0, "double precision"))') == pytest.approx(2)
        assert dbe.eval('LOG(DB_CAST(4.0, "double precision"), DB_CAST(2.0, "numeric", 10, 5))') == pytest.approx(2)


class TestMathFunctionPostgreSQL_9_3(
        PostgreSQL_9_3TestBase,
        DefaultMathFunctionFormulaConnectorTestSuite,
        PostgreSQLMatchTestMixin,
):
    pass


class TestMathFunctionPostgreSQL_9_4(
        PostgreSQL_9_4TestBase,
        DefaultMathFunctionFormulaConnectorTestSuite,
        PostgreSQLMatchTestMixin,
):
    pass
