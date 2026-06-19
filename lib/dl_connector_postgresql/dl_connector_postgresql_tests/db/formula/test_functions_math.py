import pytest

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL9p3TestBase,
    PostgreSQL9p4TestBase,
)


class PostgreSQLMatchTestMixin:
    def test_log_data_type_compatibility(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('LOG(DB_CAST(4.0, "numeric", 10, 5), DB_CAST(2.0, "double precision"))') == pytest.approx(2)
        assert dbe.eval('LOG(DB_CAST(4.0, "double precision"), DB_CAST(2.0, "numeric", 10, 5))') == pytest.approx(2)


class TestMathFunctionPostgreSQL9p3(
    PostgreSQL9p3TestBase,
    DefaultMathFunctionFormulaConnectorTestSuite,
    PostgreSQLMatchTestMixin,
):
    pass


class TestMathFunctionPostgreSQL9p4(
    PostgreSQL9p4TestBase,
    DefaultMathFunctionFormulaConnectorTestSuite,
    PostgreSQLMatchTestMixin,
):
    pass
